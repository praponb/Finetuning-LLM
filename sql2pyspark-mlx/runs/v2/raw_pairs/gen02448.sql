SELECT
    status,
    channel,
    COUNT(DISTINCT amount) AS unique_channels,
    MIN(amount) AS min_amount
FROM orders
WHERE channel IN ('store', 'mobile', 'partner') AND customer_id = 30
GROUP BY status, channel
ORDER BY min_amount DESC, unique_channels DESC;
