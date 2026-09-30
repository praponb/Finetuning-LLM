SELECT
    id,
    status,
    cost,
    order_id
FROM (
    SELECT
        id,
        status,
        cost,
        order_id,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost, id) AS row_num
    FROM shipments
) AS r
WHERE row_num = 1
ORDER BY order_id ASC
