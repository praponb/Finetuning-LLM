SELECT
    id,
    channel,
    discount,
    MAX(discount) OVER (PARTITION BY channel) AS channel_max_discount
FROM orders
WHERE channel = 'web';
