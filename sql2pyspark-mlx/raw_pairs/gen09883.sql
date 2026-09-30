SELECT region, city, CASE WHEN region = 'East' THEN 1 ELSE 0 END AS is_east
FROM stores
WHERE city IN ('Tokyo', 'Singapore')
ORDER BY region DESC, 2;
