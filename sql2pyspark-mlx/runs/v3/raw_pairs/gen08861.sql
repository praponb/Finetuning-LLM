SELECT customer_id, channel, AVG(discount) avg_discount
FROM orders
GROUP BY customer_id, channel
ORDER BY channel, avg_discount DESC;
