SELECT id, store_id, shipping_fee
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY shipping_fee, id) AS pos
    FROM orders
) AS ranked
WHERE pos <= 2
