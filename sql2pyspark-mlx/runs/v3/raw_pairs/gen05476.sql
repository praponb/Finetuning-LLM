WITH valid_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE rating IS NOT NULL OR country = 'JP'
),
rating_bucket_summary AS (
    SELECT CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 2.5 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(DISTINCT rating) AS unique_citys, ROUND((100 * SUM(CASE WHEN country = 'TH' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_th
    FROM valid_suppliers
    GROUP BY CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 2.5 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM rating_bucket_summary
WHERE pct_th < 45
ORDER BY pct_th
