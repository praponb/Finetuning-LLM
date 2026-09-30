SELECT
    id,
    shipped_date,
    carrier,
    order_id
FROM shipments
WHERE shipped_date = (SELECT MAX(shipped_date) FROM shipments);
