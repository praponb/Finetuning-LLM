SELECT id, revenue, CASE WHEN units >= 38 THEN 'High' WHEN units >= 12 THEN 'Medium' ELSE 'Low' END AS units_tier
FROM store_sales
WHERE revenue < 3900
ORDER BY revenue, id
LIMIT 3;
