SELECT
    id,
    order_id,
    delivered_date,
    carrier
FROM (
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY delivered_date DESC) AS pos
    FROM shipments
) AS ranked
WHERE pos <= 1
