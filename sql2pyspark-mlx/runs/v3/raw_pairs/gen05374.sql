SELECT * FROM (SELECT status, method, amount FROM payments)
PIVOT (AVG(amount) FOR method IN ('paypal', 'card', 'cash'));
