SELECT
    id,
    channel,
    status,
    amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY channel, status), 3) AS amount_share
FROM orders
ORDER BY channel;
