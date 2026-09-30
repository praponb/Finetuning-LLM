SELECT channel, status, AVG(discount) AS avg_discount FROM orders GROUP BY channel, status
