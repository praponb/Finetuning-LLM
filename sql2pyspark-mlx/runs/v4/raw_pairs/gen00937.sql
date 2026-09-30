SELECT * FROM (SELECT channel, status, discount FROM orders) PIVOT (AVG(discount) FOR status IN ('RETURNED', 'PENDING', 'CANCELLED'))
