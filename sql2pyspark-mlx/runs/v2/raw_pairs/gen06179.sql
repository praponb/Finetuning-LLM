SELECT opened_date, store_name, CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END AS is_singapore, CEIL(sqft) AS sqft_whole FROM stores
