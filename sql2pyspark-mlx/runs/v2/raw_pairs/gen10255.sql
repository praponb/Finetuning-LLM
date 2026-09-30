SELECT
    id,
    carrier,
    order_id,
    cost,
    shipped_date,
    MIN(cost) OVER (PARTITION BY carrier, order_id) AS carrier_min_cost,
    LEAD(cost, 1, 0) OVER (PARTITION BY order_id ORDER BY shipped_date, id) AS next_cost
FROM shipments
WHERE status IN ('LOST', 'DELIVERED')
