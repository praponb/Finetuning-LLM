SELECT
    id,
    order_id,
    delivered_date,
    cost
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date DESC, id) rnk
    FROM shipments
    WHERE carrier = 'UPS'
) r
WHERE rnk <= 2
