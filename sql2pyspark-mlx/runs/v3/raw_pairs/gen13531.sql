WITH base AS (
    SELECT *
    FROM suppliers
    WHERE supplier_name LIKE '%Wholesale%' OR city = 'Singapore'
),
summary AS (
    SELECT CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END AS rating_bucket, MIN(rating) AS min_rating
    FROM base
    GROUP BY CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END
)
SELECT rating_bucket, min_rating
FROM summary
WHERE min_rating >= 2.7
ORDER BY rating_bucket
LIMIT 5;
