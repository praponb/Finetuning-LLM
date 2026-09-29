SELECT id, store_name, opened_date, CASE WHEN sqft >= 17563 THEN 'High' WHEN sqft >= 4958 THEN 'Medium' ELSE 'Low' END AS sqft_band FROM stores WHERE LOWER(store_name) LIKE '%ive%';
