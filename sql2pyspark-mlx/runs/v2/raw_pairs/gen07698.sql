SELECT CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 2.2 THEN 'mid' ELSE 'low' END AS rating_bucket, MIN(rating) AS min_rating, AVG(rating) AS avg_rating
FROM suppliers
WHERE supplier_name LIKE 'Pac%' AND rating >= 2.4
GROUP BY CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 2.2 THEN 'mid' ELSE 'low' END
HAVING AVG(rating) >= 2.2;
