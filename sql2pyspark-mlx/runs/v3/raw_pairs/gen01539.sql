SELECT * FROM (SELECT status, method, amount FROM payments)
PIVOT (MAX(amount) FOR method IN ('bank_transfer', 'cash', 'card', 'promptpay'))
ORDER BY status;
