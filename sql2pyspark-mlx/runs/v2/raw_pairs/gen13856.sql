SELECT
    id,
    store_id,
    shipping_fee,
    amount,
    FIRST_VALUE(amount) OVER (PARTITION BY store_id ORDER BY shipping_fee DESC, id) AS first_amount
FROM orders
