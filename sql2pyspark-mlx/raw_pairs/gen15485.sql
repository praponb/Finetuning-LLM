WITH metrics_long AS (
    -- Native SQL UNPIVOT (wide to long)
    SELECT *
    FROM (
        SELECT
            id,
            supplier_id,
            cost,
            price
        FROM products
    ) src
    UNPIVOT (
        value FOR metric IN (cost, price)
    )
)
-- Grouped metrics
SELECT
    metric,
    PERCENTILE_APPROX(value, 0.95) AS value_p95,
    SUM(CASE WHEN metric = 'cost' THEN value ELSE 0 END) AS cost_value,
    ROUND(AVG(value), 2) AS avg_value
FROM metrics_long
GROUP BY metric
ORDER BY cost_value DESC NULLS FIRST
