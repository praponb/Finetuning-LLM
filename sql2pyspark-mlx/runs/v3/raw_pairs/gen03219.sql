SELECT CASE WHEN rating >= 3.9 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(rating) AS total_rating, ROUND(STDDEV(rating), 2) AS stddev_rating
FROM suppliers
WHERE rating < 3.9 OR city IN ('Singapore', 'London')
GROUP BY 1
HAVING SUM(rating) >= 23
ORDER BY rating_bucket ASC, stddev_rating ASC
