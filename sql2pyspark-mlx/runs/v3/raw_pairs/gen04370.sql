SELECT
    channel,
    COUNT(DISTINCT shipping_fee) AS unique_statuss,
    ROUND(STDDEV(discount), 2) AS stddev_discount,
    ROUND(STDDEV(shipping_fee), 2) AS stddev_shipping_fee
FROM orders
WHERE channel = 'store'
GROUP BY channel
ORDER BY stddev_discount DESC, stddev_shipping_fee DESC
