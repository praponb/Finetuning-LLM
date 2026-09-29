SELECT
    id,
    store_id,
    shipping_fee,
    channel,
    amount,
    MIN(shipping_fee) OVER (PARTITION BY store_id) AS store_id_min_shipping_fee,
    ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount DESC, id) AS row_num
FROM orders
WHERE customer_id IS NOT NULL;
