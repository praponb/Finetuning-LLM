SELECT * FROM (SELECT method, status, amount FROM payments)
PIVOT (AVG(amount) FOR status IN ('REFUNDED', 'SUCCESS', 'FAILED'))
