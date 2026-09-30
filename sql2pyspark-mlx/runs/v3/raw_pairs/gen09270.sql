SELECT
    id,
    store_id,
    amount,
    channel,
    RANK() OVER (PARTITION BY store_id ORDER BY amount) AS rnk,
    COUNT(*) OVER (PARTITION BY channel) AS channel_count
FROM orders
WHERE customer_id = 28;
