SELECT
    channel,
    SUM(shipping_fee) AS total_shipping_fee,
    COUNT(*) AS cnt,
    SUM(SUM(shipping_fee)) OVER (ORDER BY channel) AS cumulative_shipping_fee
FROM orders
GROUP BY channel
ORDER BY channel, total_shipping_fee DESC
