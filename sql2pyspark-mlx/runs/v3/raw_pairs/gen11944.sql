SELECT channel, MAX(shipping_fee) AS max_shipping_fee, MIN(order_date) AS min_order_date
FROM orders
GROUP BY channel
HAVING MAX(shipping_fee) >= 39
ORDER BY min_order_date;
