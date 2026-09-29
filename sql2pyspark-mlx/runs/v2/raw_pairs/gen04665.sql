SELECT CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(rating) AS total_rating, SUM(SUM(rating)) OVER (ORDER BY CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END) AS cumulative_rating
FROM suppliers
GROUP BY CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END
ORDER BY cumulative_rating, rating_bucket
