SELECT status, ROUND(AVG(amount), 2) AS avg_amount FROM payments WHERE amount <= 1200 AND (paid_at < '2023-07-01' OR order_id IN (14, 29, 30)) GROUP BY 1 ORDER BY 2 DESC, status
