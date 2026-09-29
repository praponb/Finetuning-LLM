SELECT
    id,
    status,
    order_date,
    shipping_fee,
    amount,
    shipping_fee - LAG(shipping_fee, 2, 0) OVER (PARTITION BY status ORDER BY order_date, id) AS shipping_fee_change,
    LAG(amount, 2) OVER (PARTITION BY status ORDER BY order_date, id) AS prev_amount
FROM orders
ORDER BY shipping_fee ASC
