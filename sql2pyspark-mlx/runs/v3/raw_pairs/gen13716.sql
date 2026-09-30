SELECT
    id,
    order_id,
    delivered_date,
    cost,
    SUM(cost) OVER (PARTITION BY order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
FROM shipments
WHERE delivered_date BETWEEN '2023-08-27' AND '2025-04-01';
