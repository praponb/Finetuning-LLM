SELECT * FROM (SELECT channel, status, amount FROM orders)
PIVOT (SUM(amount) FOR status IN ('PENDING', 'COMPLETED', 'RETURNED', 'CANCELLED'))
ORDER BY channel
