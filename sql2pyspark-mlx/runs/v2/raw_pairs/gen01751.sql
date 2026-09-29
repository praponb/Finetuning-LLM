SELECT * FROM (SELECT channel, status, amount FROM orders) PIVOT (AVG(amount) FOR status IN ('PENDING', 'SHIPPED'))
