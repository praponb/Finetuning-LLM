SELECT
    id,
    order_id,
    delivered_date,
    cost,
    LEAD(cost, 2) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS next_cost
FROM shipments
WHERE shipped_date >= '2023-10-01';
