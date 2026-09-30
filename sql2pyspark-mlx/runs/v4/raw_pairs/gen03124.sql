SELECT status, SUM(discount) total_discount, COUNT(*) cnt, SUM(SUM(discount)) OVER (ORDER BY status) AS cumulative_discount FROM orders GROUP BY status
