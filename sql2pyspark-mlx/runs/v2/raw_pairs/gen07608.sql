SELECT * FROM (SELECT status, method, amount FROM payments) PIVOT (SUM(amount) FOR method IN ('cash', 'paypal', 'bank_transfer')) ORDER BY status;
