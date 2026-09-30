SELECT
    id,
    status,
    amount,
    order_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS pos
    FROM orders
) AS r
WHERE pos <= 3
ORDER BY order_date
