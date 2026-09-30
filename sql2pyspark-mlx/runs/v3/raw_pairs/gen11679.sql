SELECT
    channel,
    MONTH(order_date) AS order_month,
    ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee,
    MIN(shipping_fee) AS min_shipping_fee
FROM orders
GROUP BY channel, order_month
ORDER BY avg_shipping_fee DESC
