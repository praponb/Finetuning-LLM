SELECT * FROM (SELECT channel, status, discount FROM orders WHERE customer_id = 24)
PIVOT (SUM(discount) FOR status IN ('COMPLETED', 'CANCELLED', 'PENDING', 'RETURNED'))
ORDER BY channel
