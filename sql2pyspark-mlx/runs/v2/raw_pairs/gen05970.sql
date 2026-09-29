SELECT id, status, amount, AVG(amount) OVER (PARTITION BY status) AS status_avg_amount FROM payments WHERE amount BETWEEN 470 AND 1570 ORDER BY status DESC;
