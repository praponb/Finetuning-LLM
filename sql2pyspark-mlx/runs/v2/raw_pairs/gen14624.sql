SELECT * FROM stores WHERE region IS NOT NULL AND (store_name LIKE '%Old%' OR city != 'Tokyo') ORDER BY 1, store_name LIMIT 1
