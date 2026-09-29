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
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date DESC, id) rn
    FROM shipments
) t
WHERE rn <= 2
ORDER BY order_id;
