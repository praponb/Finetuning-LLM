SELECT
    id,
    order_id,
    cost,
    shipped_date
FROM (
    SELECT
        id,
        order_id,
        cost,
        shipped_date,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost DESC, id) AS rn
    FROM shipments
) AS r
WHERE rn <= 5
