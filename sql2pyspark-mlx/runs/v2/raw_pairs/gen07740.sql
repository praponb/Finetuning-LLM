SELECT id, store_name, LOWER(region) AS region_lower FROM stores WHERE LOWER(store_name) LIKE '%iam%' AND opened_date >= '2024-06-17' AND city <> 'Tokyo';
