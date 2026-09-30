SELECT channel, YEAR(order_date) AS order_year, ROUND(AVG(discount), 2) AS avg_discount
FROM orders
GROUP BY channel, order_year
HAVING ROUND(AVG(discount), 2) >= 71
ORDER BY order_year DESC, channel;
