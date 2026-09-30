SELECT * FROM (SELECT status, method, amount FROM payments WHERE HOUR(paid_at) BETWEEN 7 AND 13) PIVOT (MAX(amount) FOR method IN ('card', 'bank_transfer', 'promptpay', 'paypal'));
