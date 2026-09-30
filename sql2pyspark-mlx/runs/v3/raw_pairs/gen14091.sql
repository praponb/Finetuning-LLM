SELECT * FROM (SELECT status, method, amount FROM payments)
PIVOT (SUM(amount) FOR method IN ('card', 'bank_transfer', 'promptpay', 'cash'))
ORDER BY status
