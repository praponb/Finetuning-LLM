SELECT customer_id, MAX(amount) - MIN(amount) AS amount_range, ROUND(AVG(discount), 2) AS avg_discount
FROM orders
GROUP BY customer_id
ORDER BY avg_discount DESC
