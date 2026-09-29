SELECT method, SUM(amount) AS total_amount, SUM(SUM(amount)) OVER (ORDER BY method) AS cumulative_amount FROM payments WHERE amount > 1460 GROUP BY method ORDER BY total_amount DESC, method DESC
