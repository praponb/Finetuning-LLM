SELECT CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 1.9 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(*) AS row_count
FROM suppliers
WHERE city = 'Berlin'
GROUP BY CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 1.9 THEN 'mid' ELSE 'low' END
HAVING COUNT(*) >= 1
ORDER BY rating_bucket
LIMIT 50;
