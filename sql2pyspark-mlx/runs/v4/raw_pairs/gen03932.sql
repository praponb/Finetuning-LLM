SELECT channel, MAX(order_date) AS last_order_date
FROM orders
GROUP BY channel
ORDER BY channel, last_order_date;
