WITH top_orders AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipping_fee, id) AS pos
    FROM orders
)
SELECT id, status, shipping_fee
FROM top_orders
WHERE pos = 1;
