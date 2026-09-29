SELECT
    id,
    cost,
    shipped_date,
    carrier
FROM shipments
WHERE delivered_date < '2025-04-11' AND order_id = 31 AND cost <= 63;
