SELECT CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 1.8 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(rating) AS total_rating, COUNT(DISTINCT rating) AS unique_citys
FROM suppliers
WHERE rating <= 2.6 AND city IS NULL
GROUP BY 1
HAVING SUM(CASE WHEN country = 'US' THEN rating ELSE 0 END) > 55
ORDER BY unique_citys DESC, total_rating
