SELECT id, city, opened_date, TRIM(store_name) AS clean_store_name, LENGTH(region) AS region_length FROM stores WHERE UPPER(store_name) LIKE '%LD %' AND opened_date < '2023-07-01'
