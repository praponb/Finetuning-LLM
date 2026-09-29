SELECT * FROM (SELECT method, status, amount FROM payments) PIVOT (MAX(amount) FOR status IN ('REFUNDED', 'FAILED')) ORDER BY method;
