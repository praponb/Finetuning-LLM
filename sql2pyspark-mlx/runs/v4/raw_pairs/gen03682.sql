SELECT status, SUM(amount) total_amount, SUM(SUM(amount)) OVER (ORDER BY status) AS cumulative_amount FROM payments GROUP BY status ORDER BY total_amount, status LIMIT 10;
