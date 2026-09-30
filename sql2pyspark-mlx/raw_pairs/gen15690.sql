SELECT
    id,
    order_id,
    delivered_date,
    status
FROM (
    SELECT
        id,
        order_id,
        delivered_date,
        status,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date DESC, id) AS rn
    FROM shipments
) AS r
WHERE rn <= 3
ORDER BY order_id DESC
