SELECT id, opened_date, store_name, sqft, TRIM(store_name) AS clean_store_name FROM stores WHERE region IN ('North', 'West', 'East') AND opened_date < '2025-05-01'
