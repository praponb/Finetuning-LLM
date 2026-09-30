SELECT rating_bucket, avg_rating, total_rating
FROM (
    SELECT CASE WHEN rating >= 3.7 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END AS rating_bucket, AVG(rating) AS avg_rating, SUM(rating) AS total_rating
    FROM suppliers
    WHERE supplier_name IS NOT NULL
    GROUP BY CASE WHEN rating >= 3.7 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END
) AS x
WHERE avg_rating > 3.2;
