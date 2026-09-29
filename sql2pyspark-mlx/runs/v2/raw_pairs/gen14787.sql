SELECT
    id,
    store_id,
    customer_id,
    shipping_fee + amount AS amount_with_shipping
FROM orders
ORDER BY customer_id, store_id;
