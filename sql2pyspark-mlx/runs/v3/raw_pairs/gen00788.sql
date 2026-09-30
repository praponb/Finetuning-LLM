SELECT
    id,
    order_id,
    shipped_date,
    delivered_date
FROM (
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY shipped_date) AS pos
    FROM shipments
) AS ranked
WHERE pos <= 3;
