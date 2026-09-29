SELECT status, channel, MIN(amount) AS min_amount, SUM(discount) AS total_discount, AVG(discount) AS avg_discount FROM orders GROUP BY status, channel HAVING MIN(amount) >= 1170 ORDER BY min_amount
