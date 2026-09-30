SELECT
    id,
    order_id,
    carrier,
    shipped_date,
    ROW_NUMBER() OVER (PARTITION BY order_id, carrier ORDER BY shipped_date, id) AS row_num
FROM shipments;
