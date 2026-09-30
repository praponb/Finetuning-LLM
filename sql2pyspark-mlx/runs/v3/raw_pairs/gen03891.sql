SELECT id, store_name, COALESCE(sqft, 0) AS sqft_filled FROM stores WHERE region IS NULL AND city IN ('Bangkok', 'New York', 'Singapore') AND region <> 'North'
