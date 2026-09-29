WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(status) AS status_upper,
        CAST(cost AS INT) AS cost_int
    FROM shipments
    WHERE shipped_date > DATE '2023-07-14'
)
-- Window metrics over several different partitions
SELECT
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    MIN(cost_int) OVER (PARTITION BY carrier, order_id ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_cost_int,
    SUM(cost_int) OVER (PARTITION BY carrier, order_id ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_cost_int,
    AVG(cost_int) OVER (PARTITION BY carrier, order_id ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_cost_int
FROM base
WHERE shipped_date <= '2025-10-01'
