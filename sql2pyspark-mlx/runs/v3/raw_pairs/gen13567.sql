WITH Base AS (
    SELECT *
    FROM suppliers
    WHERE supplier_name LIKE '%Globa%' AND country IN ('SG', 'US') AND LOWER(supplier_name) LIKE '%aci%'
),
rating_bucket_summary AS (
    SELECT CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END AS rating_bucket, city, COUNT(rating) AS rating_count
    FROM Base
    GROUP BY CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END, city
)
SELECT *
FROM rating_bucket_summary
WHERE rating_count >= 9;
