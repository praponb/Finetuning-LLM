SELECT * FROM (SELECT method, status, amount FROM payments) PIVOT (COUNT(amount) FOR status IN ('SUCCESS', 'REFUNDED', 'FAILED')) ORDER BY method;
