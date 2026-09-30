WITH valid_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE rating > 3.5
),
agg AS (
    SELECT CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 1.9 THEN 'mid' ELSE 'low' END AS rating_bucket, ROUND((100 * SUM(CASE WHEN country = 'US' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_us
    FROM valid_suppliers
    GROUP BY CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 1.9 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM agg
ORDER BY pct_us DESC;
