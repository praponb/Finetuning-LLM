SELECT
    status,
    carrier,
    delivered_cost,
    unique_orders
FROM (
    SELECT
        status,
        carrier,
        SUM(CASE WHEN status = 'DELIVERED' THEN cost ELSE 0 END) AS delivered_cost,
        COUNT(DISTINCT cost) AS unique_orders
    FROM shipments
    GROUP BY status, carrier
) AS x
WHERE delivered_cost < 69
