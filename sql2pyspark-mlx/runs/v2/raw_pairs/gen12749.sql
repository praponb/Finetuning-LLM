SELECT city, TRIM(store_name) AS clean_store_name FROM stores WHERE opened_date >= '2024-10-01' AND MONTH(opened_date) IN (10, 11, 12)
