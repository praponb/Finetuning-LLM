SELECT CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(rating) AS total_rating, SUM(SUM(rating)) OVER (ORDER BY CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END) AS cumulative_rating
FROM suppliers
WHERE city != 'Berlin'
GROUP BY CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END
ORDER BY rating_bucket
