SELECT *, TRIM(city) AS clean_city FROM stores WHERE city IN ('Bangkok', 'New York', 'Chiang Mai') AND sqft < 7327;
