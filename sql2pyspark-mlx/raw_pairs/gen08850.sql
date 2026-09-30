SELECT
    id,
    status,
    discount,
    store_id
FROM orders
WHERE customer_id IS NOT NULL AND shipping_fee IS NOT NULL
ORDER BY id, status DESC;
