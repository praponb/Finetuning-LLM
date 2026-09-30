SELECT * FROM (SELECT method, status, amount FROM payments WHERE status IN ('FAILED', 'SUCCESS'))
PIVOT (COUNT(amount) FOR status IN ('SUCCESS', 'REFUNDED', 'FAILED'))
