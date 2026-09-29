SELECT
    id,
    order_id,
    shipped_date,
    status
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date DESC, id) row_num
    FROM shipments
    WHERE cost <= 80
) r
WHERE row_num = 1;
