SELECT * FROM (SELECT city, region, sqft FROM stores WHERE UPPER(TRIM(store_name)) = 'OLD TOWN')
PIVOT (MAX(sqft) FOR region IN ('South', 'East'))
ORDER BY city;
