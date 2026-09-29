WITH base AS (
    SELECT *
    FROM store_sales
    WHERE product_id IN (5, 7)
),
agg AS (
    SELECT
        store_id,
        CASE WHEN revenue >= 3300 THEN 'high' WHEN revenue >= 1100 THEN 'mid' ELSE 'low' END AS revenue_bucket,
        AVG(units) AS avg_units,
        MIN(revenue) AS min_revenue
    FROM base
    GROUP BY store_id, CASE WHEN revenue >= 3300 THEN 'high' WHEN revenue >= 1100 THEN 'mid' ELSE 'low' END
)
SELECT store_id, revenue_bucket, avg_units
FROM agg
WHERE min_revenue < 3300
