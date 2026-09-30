SELECT
    order_id,
    SUM(cost) AS total_cost,
    COUNT(*) AS cnt,
    ROUND(SUM(cost) * 100.0 / SUM(SUM(cost)) OVER (), 2) AS pct_of_total
FROM shipments
WHERE status NOT IN ('RETURNED', 'DELIVERED')
GROUP BY order_id;
