SELECT
    id,
    delivered_date,
    carrier,
    order_id,
    ROUND(cost, 0) AS cost_rounded
FROM shipments
WHERE delivered_date < '2023-06-01' AND shipped_date >= '2023-08-01';
