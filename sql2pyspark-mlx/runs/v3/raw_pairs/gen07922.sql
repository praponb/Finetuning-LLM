SELECT
    id,
    order_id,
    shipped_date,
    status
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date DESC, id) AS pos
    FROM shipments
) AS t
WHERE pos = 1
ORDER BY status DESC, shipped_date
