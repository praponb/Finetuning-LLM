SELECT * FROM (SELECT channel, status, amount FROM orders WHERE status <> 'PENDING')
PIVOT (COUNT(amount) FOR status IN ('RETURNED', 'SHIPPED', 'PENDING'))
ORDER BY channel
