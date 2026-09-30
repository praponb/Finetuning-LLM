SELECT id, channel, order_date, amount, amount - LAG(amount, 1) OVER (PARTITION BY channel ORDER BY order_date, id) AS amount_change FROM orders WHERE order_date BETWEEN '2024-06-01' AND '2024-11-01'
