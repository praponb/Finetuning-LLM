SELECT *
FROM suppliers
WHERE city IN ('London', 'Tokyo') AND country IN ('SG', 'TH')
ORDER BY 1
LIMIT 1
