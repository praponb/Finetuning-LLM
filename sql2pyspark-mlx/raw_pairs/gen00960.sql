SELECT
    customer_id,
    amount,
    discount,
    UPPER(channel) AS channel_upper
FROM orders
ORDER BY discount DESC;
