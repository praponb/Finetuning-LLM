SELECT
    MONTH(order_date) AS order_month,
    SUM(shipping_fee) AS total_shipping_fee,
    COUNT(*) AS cnt,
    SUM(SUM(shipping_fee)) OVER (ORDER BY MONTH(order_date)) AS cumulative_shipping_fee
FROM orders
GROUP BY MONTH(order_date)
ORDER BY total_shipping_fee DESC, order_month;
