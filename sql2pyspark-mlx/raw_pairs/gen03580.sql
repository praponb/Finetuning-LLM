SELECT * FROM (SELECT YEAR(paid_at) AS paid_year, method, amount FROM payments) PIVOT (MAX(amount) FOR method IN ('bank_transfer', 'cash')) ORDER BY paid_year;
