SELECT * FROM (SELECT status, method, amount FROM payments WHERE HOUR(paid_at) BETWEEN 9 AND 16) PIVOT (SUM(amount) FOR method IN ('cash', 'paypal', 'bank_transfer'));
