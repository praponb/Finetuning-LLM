SELECT sqft, region FROM stores WHERE UPPER(TRIM(store_name)) = 'OLD TOWN' UNION ALL SELECT sqft, region FROM stores WHERE sqft > 5552 ORDER BY region
