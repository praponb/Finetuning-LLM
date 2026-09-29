SELECT method, SUM(amount) AS total_amount, COUNT(*) AS cnt, SUM(SUM(amount)) OVER (ORDER BY method) AS cumulative_amount FROM payments GROUP BY method ORDER BY method ASC, cnt DESC
