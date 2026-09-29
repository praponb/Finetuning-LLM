SELECT id, city, sqft, ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 3) sqft_share FROM stores WHERE city IN ('Singapore', 'Berlin')
