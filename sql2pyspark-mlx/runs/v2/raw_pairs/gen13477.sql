WITH ValidSuppliers AS (
    SELECT *, REGEXP_REPLACE(supplier_name, ' ', '-') AS supplier_name_slug
    FROM suppliers
    WHERE city = 'Bangkok'
),
rating_bucket_summary AS (
    SELECT CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, city, SUM(rating) AS total_rating
    FROM ValidSuppliers
    GROUP BY CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END, city
)
SELECT rating_bucket, total_rating
FROM rating_bucket_summary
WHERE total_rating < 23
ORDER BY total_rating DESC, rating_bucket;
