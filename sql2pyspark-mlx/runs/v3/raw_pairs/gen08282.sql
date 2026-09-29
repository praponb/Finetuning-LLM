SELECT
    id,
    store_id,
    shipping_fee,
    amount,
    MIN(shipping_fee) OVER (PARTITION BY store_id) AS store_id_min_shipping_fee,
    NTILE(10) OVER (ORDER BY amount DESC, id) AS quartile
FROM orders
