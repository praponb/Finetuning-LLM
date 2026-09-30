SELECT
    id,
    store_id,
    customer_id,
    shipping_fee,
    RANK() OVER (PARTITION BY store_id, customer_id ORDER BY shipping_fee DESC) AS shipping_fee_rank
FROM orders
ORDER BY id, store_id DESC
