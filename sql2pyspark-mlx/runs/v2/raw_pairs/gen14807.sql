SELECT
    id,
    channel,
    order_date,
    shipping_fee,
    LAG(shipping_fee, 1, 0) OVER (PARTITION BY channel ORDER BY order_date, id) prev_shipping_fee
FROM orders;
