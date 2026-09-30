SELECT * FROM (SELECT DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_month, method, amount FROM payments)
PIVOT (SUM(amount) FOR method IN ('promptpay', 'cash', 'card', 'paypal'))
ORDER BY paid_month;
