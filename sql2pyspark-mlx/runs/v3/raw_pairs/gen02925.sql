SELECT
    id,
    order_id,
    cost,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost, id) AS row_num
FROM shipments
