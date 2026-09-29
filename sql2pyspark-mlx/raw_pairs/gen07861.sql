SELECT
    id,
    channel,
    discount,
    store_id
FROM orders
WHERE customer_id IS NOT NULL
ORDER BY discount DESC, id
LIMIT 1
