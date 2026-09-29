SELECT
    id,
    customer_id,
    order_date,
    channel,
    LAST_VALUE(channel) OVER (PARTITION BY customer_id ORDER BY order_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_channel
FROM orders
ORDER BY order_date, channel
