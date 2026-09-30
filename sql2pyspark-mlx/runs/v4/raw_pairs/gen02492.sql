WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        quantity,
        last_restock,
        CAST(quantity AS DOUBLE) quantity_str
    FROM inventory
    WHERE quantity <= 224
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(quantity_str) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_quantity_str,
        NTILE(4) OVER (ORDER BY quantity, id) quantity_bucket
    FROM base
),
with_windows_inventory AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity,
        last_restock,
        quantity_str,
        running_quantity_str,
        quantity_bucket,
        LAG(quantity_str, 1) OVER w_part prev_quantity_str,
        AVG(quantity) OVER (ORDER BY running_quantity_str, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) quantity_avg_5_prior,
        SUM(running_quantity_str) OVER (ORDER BY running_quantity_str, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_running_quantity_str
    FROM with_windows
    WINDOW w_part AS (ORDER BY quantity, id)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN prev_quantity_str >= 76 THEN 'HIGH' ELSE 'LOW' END AS prev_quantity_str_tier,
    APPROX_PERCENTILE(running_quantity_str, ARRAY(0.25, 0.5, 0.75)) AS running_quantity_str_quartiles,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    PERCENTILE_APPROX(running_quantity_str, ARRAY(0.25, 0.5, 0.75)) AS running_quantity_str_quartiles_2,
    COUNT(*) AS cnt,
    AVG(prev_quantity_str) AS avg_prev_quantity_str
FROM with_windows_inventory
GROUP BY CASE WHEN prev_quantity_str >= 76 THEN 'HIGH' ELSE 'LOW' END
ORDER BY cnt DESC;
