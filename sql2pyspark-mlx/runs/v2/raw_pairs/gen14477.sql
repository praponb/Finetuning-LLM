SELECT rating_bucket, city, us_count
FROM (
    SELECT CASE WHEN rating >= 3.7 THEN 'high' WHEN rating >= 1.9 THEN 'mid' ELSE 'low' END AS rating_bucket, city, SUM(CASE WHEN country = 'US' THEN 1 ELSE 0 END) AS us_count
    FROM suppliers
    GROUP BY CASE WHEN rating >= 3.7 THEN 'high' WHEN rating >= 1.9 THEN 'mid' ELSE 'low' END, city
) AS sub
WHERE us_count >= 80
ORDER BY 2, rating_bucket
LIMIT 10
