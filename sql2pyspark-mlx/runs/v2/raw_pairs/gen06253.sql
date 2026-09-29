SELECT
    id,
    order_id,
    delivered_date,
    cost,
    SUM(cost) OVER (PARTITION BY order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost,
    cost - LAG(cost, 1) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS cost_change
FROM shipments
WHERE YEAR(shipped_date) = 2023
ORDER BY order_id;
