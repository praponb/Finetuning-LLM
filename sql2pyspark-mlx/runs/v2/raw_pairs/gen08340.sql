SELECT *, INITCAP(TRIM(city)) AS city_title FROM stores WHERE LOWER(store_name) LIKE '%ld %'
