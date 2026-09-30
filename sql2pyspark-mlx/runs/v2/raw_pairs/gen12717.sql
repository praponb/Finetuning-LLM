SELECT CASE WHEN rating >= 4.2 THEN 'high' WHEN rating >= 2.5 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END) AS singapore_count
FROM suppliers
GROUP BY CASE WHEN rating >= 4.2 THEN 'high' WHEN rating >= 2.5 THEN 'mid' ELSE 'low' END
ORDER BY singapore_count, rating_bucket DESC;
