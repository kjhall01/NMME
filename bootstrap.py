import requests 
import xarray as xr 
from pathlib import Path 


base_urls = { 
	"GEM5-NEMO": "https://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.CanSIPS-IC3/.GEM5-NEMO/.HINDCAST/.MONTHLY/.prec/%5BM%5D/average/", 
	"CANCM4I": "https://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.CanSIPS-IC3/.CanCM4i-IC3/.HINDCAST/.MONTHLY/.prec/%5BM%5D/average/",
	"CCSM4": "https://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.COLA-RSMAS-CCSM4/.MONTHLY/.prec/%5BM%5D/average/", 
	"CANCM3": "http://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.CMC1-CanCM3/.HINDCAST/.MONTHLY/.prec/SOURCES/.Models/.NMME/.CMC1-CanCM3/.FORECAST/.MONTHLY/.prec/appendstream/%5BM%5D/average/",
	"CANCM4": "http://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.CMC2-CanCM4/.HINDCAST/.MONTHLY/.prec/SOURCES/.Models/.NMME/.CMC2-CanCM4/.FORECAST/.MONTHLY/.prec/appendstream/%5BM%5D/average/", 
#	"GEM-NEMO": "http://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.GEM-NEMO/.HINDCAST/.MONTHLY/.prec/%5BM%5D/average/",
#	"GEM-NEMO-F": "http://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.GEM-NEMO/.FORECAST/.MONTHLY/.prec/%5BM%5D/average/",
	"GFDL-AER04": "http://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.GFDL-CM2p1-aer04/.MONTHLY/.prec/%5BM%5D/average/",
	"GFDL-A06": "http://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.GFDL-CM2p5-FLOR-A06/.MONTHLY/.prec/%5BM%5D/average/",
	"GFDL-B01": "http://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.GFDL-CM2p5-FLOR-B01/.MONTHLY/.prec/%5BM%5D/average/",
	"GFDL-SPEAR": "http://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.GFDL-SPEAR/.HINDCAST/.MONTHLY/.prec/%5BM%5D/average/S/first/(1%20Nov%202020)/RANGEEDGES/SOURCES/.Models/.NMME/.GFDL-SPEAR/.FORECAST/.MONTHLY/.prec/%5BM%5D/average/appendstream/", 
	"NASA-GEOS": "https://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.NASA-GEOSS2S/.HINDCAST/.MONTHLY/.prec/%5BM%5D/average/SOURCES/.Models/.NMME/.NASA-GEOSS2S/.FORECAST/.MONTHLY/.prec/%5BM%5D/average/appendstream/", 
	"NCEP-CFSV2": "https://iridl.ldeo.columbia.edu/SOURCES/.Models/.NMME/.NCEP-CFSv2/.HINDCAST/.PENTAD_SAMPLES_FULL/.prec/%5BM%5D/average/"
}

time_chunks = {
	"GEM5-NEMO": [ 'S/(1%20Jan%201980)/(1%20Dec%201985)/RANGEEDGES/', 'S/(1%20Jan%201986)/(1%20Dec%201990)/RANGEEDGES/', 'S/(1%20Jan%201991)/(1%20Dec%201995)/RANGEEDGES/', 'S/(1%20Jan%201996)/(1%20Dec%202000)/RANGEEDGES/', 'S/(1%20Jan%202001)/(1%20Dec%202005)/RANGEEDGES/', 'S/(1%20Jan%202006)/(1%20Dec%202010)/RANGEEDGES/', 'S/(1%20Jan%202011)/(1%20Dec%202015)/RANGEEDGES/', 'S/(1%20Jan%202016)/(1%20Dec%202020)/RANGEEDGES/' ],
	"CANCM4I": [ 'S/(1%20Jan%201980)/(1%20Dec%201985)/RANGEEDGES/', 'S/(1%20Jan%201986)/(1%20Dec%201990)/RANGEEDGES/', 'S/(1%20Jan%201991)/(1%20Dec%201995)/RANGEEDGES/', 'S/(1%20Jan%201996)/(1%20Dec%202000)/RANGEEDGES/', 'S/(1%20Jan%202001)/(1%20Dec%202005)/RANGEEDGES/', 'S/(1%20Jan%202006)/(1%20Dec%202010)/RANGEEDGES/', 'S/(1%20Jan%202011)/(1%20Dec%202015)/RANGEEDGES/', 'S/(1%20Jan%202016)/(1%20Dec%202020)/RANGEEDGES/' ],
	"CCSM4": [ 'S/(1%20Jan%201982)/(1%20Dec%201985)/RANGEEDGES/', 'S/(1%20Jan%201986)/(1%20Dec%201990)/RANGEEDGES/', 'S/(1%20Jan%201991)/(1%20Dec%201995)/RANGEEDGES/', 'S/(1%20Jan%201996)/(1%20Dec%202000)/RANGEEDGES/', 'S/(1%20Jan%202001)/(1%20Dec%202005)/RANGEEDGES/', 'S/(1%20Jan%202006)/(1%20Dec%202010)/RANGEEDGES/', 'S/(1%20Jan%202011)/(1%20Dec%202015)/RANGEEDGES/', 'S/(1%20Jan%202016)/(1%20Dec%202020)/RANGEEDGES/' ],
	"CANCM3": [ 'S/(1%20Jan%201981)/(1%20Dec%201985)/RANGEEDGES/', 'S/(1%20Jan%201986)/(1%20Dec%201990)/RANGEEDGES/', 'S/(1%20Jan%201991)/(1%20Dec%201995)/RANGEEDGES/', 'S/(1%20Jan%201996)/(1%20Dec%202000)/RANGEEDGES/', 'S/(1%20Jan%202001)/(1%20Dec%202005)/RANGEEDGES/', 'S/(1%20Jan%202006)/(1%20Dec%202010)/RANGEEDGES/', 'S/(1%20Jan%202011)/(1%20Dec%202015)/RANGEEDGES/', 'S/(1%20Jan%202016)/(1%20Jul%202019)/RANGEEDGES/' ],
	"CANCM4": [ 'S/(1%20Jan%201981)/(1%20Dec%201985)/RANGEEDGES/', 'S/(1%20Jan%201986)/(1%20Dec%201990)/RANGEEDGES/', 'S/(1%20Jan%201991)/(1%20Dec%201995)/RANGEEDGES/', 'S/(1%20Jan%201996)/(1%20Dec%202000)/RANGEEDGES/', 'S/(1%20Jan%202001)/(1%20Dec%202005)/RANGEEDGES/', 'S/(1%20Jan%202006)/(1%20Dec%202010)/RANGEEDGES/', 'S/(1%20Jan%202011)/(1%20Dec%202015)/RANGEEDGES/', 'S/(1%20Jan%202016)/(1%20Jul%202019)/RANGEEDGES/' ],
#	"GEM-NEMO": [ 'S/(1%20Jan%201981)/(1%20Dec%201985)/RANGEEDGES/', 'S/(1%20Jan%201986)/(1%20Dec%201990)/RANGEEDGES/', 'S/(1%20Jan%201991)/(1%20Dec%201995)/RANGEEDGES/', 'S/(1%20Jan%201996)/(1%20Dec%202000)/RANGEEDGES/', 'S/(1%20Jan%202001)/(1%20Dec%202005)/RANGEEDGES/', 'S/(1%20Jan%202006)/(1%20Dec%202010)/RANGEEDGES/', 'S/(1%20Jan%202011)/(1%20Dec%202015)/RANGEEDGES/', 'S/(1%20Jan%202016)/(1%20Dec%202018)/RANGEEDGES/' ],
#	"GEM-NEMO-F": [  'S/(1%20Jan%202019)/(1%20Nov%202021)/RANGEEDGES/' ],
	"GFDL-AER04": [ 'S/(1%20Jan%201982)/(1%20Dec%201985)/RANGEEDGES/', 'S/(1%20Jan%201986)/(1%20Dec%201990)/RANGEEDGES/', 'S/(1%20Jan%201991)/(1%20Dec%201995)/RANGEEDGES/', 'S/(1%20Jan%201996)/(1%20Dec%202000)/RANGEEDGES/', 'S/(1%20Jan%202001)/(1%20Dec%202005)/RANGEEDGES/', 'S/(1%20Jan%202006)/(1%20Dec%202010)/RANGEEDGES/', 'S/(1%20Jan%202011)/(1%20Dec%202015)/RANGEEDGES/', 'S/(1%20Jan%202016)/(1%20Jan%202021)/RANGEEDGES/' ],
	"GFDL-B01":  [ 'S/(1%20Mar%201980)/(1%20Dec%201985)/RANGEEDGES/', 'S/(1%20Jan%201986)/(1%20Dec%201990)/RANGEEDGES/', 'S/(1%20Jan%201991)/(1%20Dec%201995)/RANGEEDGES/', 'S/(1%20Jan%201996)/(1%20Dec%202000)/RANGEEDGES/', 'S/(1%20Jan%202001)/(1%20Dec%202005)/RANGEEDGES/', 'S/(1%20Jan%202006)/(1%20Dec%202010)/RANGEEDGES/', 'S/(1%20Jan%202011)/(1%20Dec%202015)/RANGEEDGES/', 'S/(1%20Jan%202016)/(1%20Jan%202021)/RANGEEDGES/' ],
	"GFDL-A06": [ 'S/(1%20Mar%201980)/(1%20Dec%201985)/RANGEEDGES/', 'S/(1%20Jan%201986)/(1%20Dec%201990)/RANGEEDGES/', 'S/(1%20Jan%201991)/(1%20Dec%201995)/RANGEEDGES/', 'S/(1%20Jan%201996)/(1%20Dec%202000)/RANGEEDGES/', 'S/(1%20Jan%202001)/(1%20Dec%202005)/RANGEEDGES/', 'S/(1%20Jan%202006)/(1%20Dec%202010)/RANGEEDGES/', 'S/(1%20Jan%202011)/(1%20Dec%202015)/RANGEEDGES/', 'S/(1%20Jan%202016)/(1%20Jan%202021)/RANGEEDGES/' ],
	"GFDL-SPEAR": [ 'S/(1%20Jan%201991)/(1%20Dec%201995)/RANGEEDGES/', 'S/(1%20Jan%201996)/(1%20Dec%202000)/RANGEEDGES/', 'S/(1%20Jan%202001)/(1%20Dec%202005)/RANGEEDGES/', 'S/(1%20Jan%202006)/(1%20Dec%202010)/RANGEEDGES/', 'S/(1%20Jan%202011)/(1%20Dec%202015)/RANGEEDGES/', 'S/(1%20Jan%202016)/(1%20Dec%202020)/RANGEEDGES/' ],
	"NASA-GEOS": [ 'S/(1%20Feb%201981)/(1%20Dec%201985)/RANGEEDGES/', 'S/(1%20Jan%201986)/(1%20Dec%201990)/RANGEEDGES/', 'S/(1%20Jan%201991)/(1%20Dec%201995)/RANGEEDGES/', 'S/(1%20Jan%201996)/(1%20Dec%202000)/RANGEEDGES/', 'S/(1%20Jan%202001)/(1%20Dec%202005)/RANGEEDGES/', 'S/(1%20Jan%202006)/(1%20Dec%202010)/RANGEEDGES/', 'S/(1%20Jan%202011)/(1%20Dec%202015)/RANGEEDGES/', 'S/(1%20Jan%202016)/(1%20Dec%202021)/RANGEEDGES/' ],
	"NCEP-CFSV2": [ 'S/(1%20Jan%201982)/(1%20Dec%201985)/RANGEEDGES/', 'S/(1%20Jan%201986)/(1%20Dec%201990)/RANGEEDGES/', 'S/(1%20Jan%201991)/(1%20Dec%201995)/RANGEEDGES/', 'S/(1%20Jan%201996)/(1%20Dec%202000)/RANGEEDGES/', 'S/(1%20Jan%202001)/(1%20Dec%202005)/RANGEEDGES/', 'S/(1%20Jan%202006)/(1%20Dec%202010)/RANGEEDGES/', 'S/(1%20Jan%202011)/(1%20Dec%202015)/RANGEEDGES/', 'S/(1%20Jan%202016)/(1%20Dec%202021)/RANGEEDGES/' ]
}



def install(model): 
	base_url = base_urls[model]
	chunks=[]
	for i, chunk in enumerate(time_chunks[model]): 
		print(model + ' ' + str(i) ) 
		if not Path(f"{model}_{i}.nc").is_file():
			url = base_url + chunk + 'data.nc' 
			print(url)
			r = requests.get(url)
			assert r.status_code == 200, f'Check {url}'
			with open(f"{model}_{i}.nc", 'wb') as f: 
				f.write(r.content)
			chunks.append(xr.open_dataset(f"{model}_{i}.nc", decode_times=False) )
	full = xr.concat(chunks, 'S') 
	full.S.attrs['calendar'] = '360_day'
	full = xr.decode_cf(full) 
	full.to_netcdf(f"{model}.nc") 
	for i in range(len(time_chunks[model])):
		Path(f"{model}_{i}.nc").unlink() 
	print(f'Installed {model}! Wahoo!')

for model in base_urls.keys():
	if not Path(f"{model}.nc").is_file():
		install(model) 
	


