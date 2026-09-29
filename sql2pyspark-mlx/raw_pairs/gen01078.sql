SELECT
    id,
    status,
    order_date,
    shipping_fee,
    channel,
    amount,
    ROUND(AVG(shipping_fee) OVER (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS shipping_fee_moving_avg_7,
    ROUND(AVG(amount) OVER (PARTITION BY channel ORDER BY order_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_4
FROM orders
WHERE discount > 0.3
ORDER BY amount_moving_avg_4 DESC, channel
