SELECT
    id,
    carrier,
    delivered_date,
    order_id,
    FIRST_VALUE(order_id) OVER (PARTITION BY carrier ORDER BY delivered_date, id) AS first_order_id
FROM shipments
WHERE cost BETWEEN 44 AND 53
