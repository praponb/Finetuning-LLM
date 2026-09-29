WITH valid AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        ROUND(cost * 100, 2) AS cost_scaled,
        COALESCE(carrier, 'OTHER') AS resolved_carrier
    FROM shipments
    WHERE order_id IS NOT NULL
      AND MONTH(delivered_date) IN (6, 7, 12)
)
SELECT
    id,
    shipped_date,
    delivered_date,
    cost,
    cost_scaled,
    SUM(cost_scaled) OVER (PARTITION BY order_id ORDER BY delivered_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_cost_scaled,
    COUNT(cost_scaled) OVER (PARTITION BY order_id ORDER BY delivered_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_cost_scaled,
    MAX(cost_scaled) OVER (PARTITION BY order_id ORDER BY delivered_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_cost_scaled
FROM valid;
