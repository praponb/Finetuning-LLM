SELECT channel, AVG(amount) AS avg_amount FROM orders WHERE status NOT IN ('RETURNED', 'CANCELLED', 'COMPLETED') GROUP BY channel ORDER BY avg_amount DESC;
