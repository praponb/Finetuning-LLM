SELECT method, AVG(amount) AS avg_amount, COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_count FROM payments GROUP BY method HAVING COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) > 1;
