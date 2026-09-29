SELECT id, store_name, sqft FROM stores WHERE MONTH(opened_date) IN (2, 6, 11) UNION ALL SELECT id, store_name, sqft FROM stores WHERE city IN ('Singapore', 'Tokyo', 'Bangkok')
