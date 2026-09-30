WITH filtered_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE country IN ('DE', 'SG', 'JP') AND country = 'SG' AND country != 'US'
),
summary AS (
    SELECT CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 2.6 THEN 'mid' ELSE 'low' END AS rating_bucket, MAX(rating) - MIN(rating) AS rating_range, SUM(CASE WHEN country = 'DE' THEN 1 ELSE 0 END) AS de_count
    FROM filtered_suppliers
    GROUP BY CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 2.6 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary
ORDER BY rating_bucket
LIMIT 5;
