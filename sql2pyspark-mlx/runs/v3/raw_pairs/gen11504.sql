SELECT region FROM stores WHERE store_name LIKE '%Siam%' UNION ALL SELECT region FROM stores WHERE store_name LIKE '%Riverside%' ORDER BY region;
