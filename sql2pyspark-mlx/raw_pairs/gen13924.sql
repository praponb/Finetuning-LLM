SELECT
    id,
    carrier,
    shipped_date,
    order_id
FROM (
    SELECT
        id,
        carrier,
        shipped_date,
        order_id,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date DESC, id) AS rn
    FROM shipments
    WHERE cost < 48
) AS ranked
WHERE rn <= 3
