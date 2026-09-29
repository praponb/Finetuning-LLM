SELECT * FROM (SELECT status, method, amount FROM payments)
PIVOT (SUM(amount) FOR method IN ('promptpay', 'bank_transfer', 'card'))
ORDER BY status;
