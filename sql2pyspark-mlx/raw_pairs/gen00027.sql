SELECT * FROM (SELECT status, method, amount FROM payments) PIVOT (SUM(amount) FOR method IN ('paypal', 'bank_transfer', 'cash'))
