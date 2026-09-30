SELECT region, COUNT(*) AS row_count FROM stores WHERE city IN ('New York', 'Chiang Mai') GROUP BY region ORDER BY region;
