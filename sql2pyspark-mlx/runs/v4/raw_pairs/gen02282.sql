SELECT * FROM (SELECT method, status, amount FROM payments)
PIVOT (MAX(amount) FOR status IN ('FAILED', 'SUCCESS', 'REFUNDED'))
ORDER BY method;
