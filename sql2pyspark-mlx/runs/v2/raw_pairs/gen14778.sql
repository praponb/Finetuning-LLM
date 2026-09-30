SELECT
    id,
    order_id,
    shipped_date,
    cost
FROM (
    SELECT
        id,
        order_id,
        shipped_date,
        cost,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date DESC, id) AS row_num
    FROM shipments
) AS r
WHERE row_num = 1;
