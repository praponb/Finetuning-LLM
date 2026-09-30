WITH src AS (
    SELECT *
    FROM suppliers
    WHERE city IN ('Bangkok', 'New York')
),
rating_bucket_summary AS (
    SELECT CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(rating) AS total_rating, MAX(rating) - MIN(rating) AS rating_range
    FROM src
    GROUP BY CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM rating_bucket_summary
WHERE total_rating >= 9
ORDER BY rating_bucket
LIMIT 10
