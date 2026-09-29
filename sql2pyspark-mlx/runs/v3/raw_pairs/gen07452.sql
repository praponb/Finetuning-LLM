SELECT CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END) AS singapore_count
FROM suppliers
GROUP BY CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END
HAVING singapore_count > 36
ORDER BY rating_bucket
LIMIT 5
