SELECT channel, ROUND(AVG(amount), 2) AS avg_amount FROM orders GROUP BY channel ORDER BY avg_amount DESC
