SELECT
    id,
    order_id,
    cost,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost DESC, id) AS row_num
FROM shipments
WHERE delivered_date >= '2023-05-30'
