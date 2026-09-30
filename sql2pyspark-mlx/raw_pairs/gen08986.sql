SELECT
    id,
    customer_id,
    amount,
    channel,
    shipping_fee,
    AVG(amount) OVER (PARTITION BY customer_id) AS customer_id_avg_amount,
    LAST_VALUE(shipping_fee) OVER (PARTITION BY channel ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_shipping_fee
FROM orders
