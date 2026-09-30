SELECT CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(rating) AS total_rating, RANK() OVER (ORDER BY SUM(rating) DESC) AS rnk
FROM suppliers
GROUP BY CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END
ORDER BY rnk DESC
