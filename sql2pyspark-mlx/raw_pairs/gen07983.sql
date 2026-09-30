SELECT region FROM stores WHERE sqft >= 12760 UNION ALL SELECT region FROM stores WHERE store_name LIKE 'D%' ORDER BY region
