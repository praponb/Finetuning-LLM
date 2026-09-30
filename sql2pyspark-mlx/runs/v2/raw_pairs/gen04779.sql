SELECT * FROM (SELECT status, method, amount FROM payments WHERE method IN ('promptpay', 'cash')) PIVOT (MAX(amount) FOR method IN ('promptpay', 'bank_transfer')) ORDER BY status;
