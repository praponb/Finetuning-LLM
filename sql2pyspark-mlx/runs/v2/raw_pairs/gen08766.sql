SELECT store_name, sqft, opened_date FROM stores WHERE MONTH(opened_date) IN (9, 11, 12) AND LENGTH(store_name) >= 6 AND region NOT IN ('North', 'East', 'West') ORDER BY sqft;
