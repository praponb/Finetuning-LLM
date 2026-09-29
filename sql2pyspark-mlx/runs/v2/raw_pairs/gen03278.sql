WITH filtered_stores AS (
    SELECT *
    FROM stores
    WHERE MONTH(opened_date) IN (2, 3, 6) AND (region IS NULL OR region = 'South')
),
agg AS (
    SELECT region, AVG(sqft) AS avg_sqft
    FROM filtered_stores
    GROUP BY region
)
SELECT region, avg_sqft
FROM agg
ORDER BY region DESC
