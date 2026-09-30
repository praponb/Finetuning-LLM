SELECT * FROM (SELECT status, method, amount FROM payments) PIVOT (AVG(amount) FOR method IN ('bank_transfer', 'card', 'paypal', 'cash')) ORDER BY status;
