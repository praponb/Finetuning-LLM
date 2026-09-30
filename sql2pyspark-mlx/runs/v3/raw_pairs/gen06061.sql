SELECT method, SUM(amount) AS total_amount FROM payments WHERE paid_at < '2023-09-29' GROUP BY method ORDER BY total_amount;
