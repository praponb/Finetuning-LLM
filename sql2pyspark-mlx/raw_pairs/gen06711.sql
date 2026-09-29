SELECT CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END AS rating_bucket, ROUND((100 * SUM(CASE WHEN country = 'TH' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_th, SUM(CASE WHEN city = 'Chiang Mai' THEN rating ELSE 0 END) AS chiang_mai_rating
FROM suppliers
WHERE city IN ('New York', 'Berlin', 'London') AND country != 'US'
GROUP BY CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END
ORDER BY rating_bucket
