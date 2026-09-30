SELECT * FROM (SELECT status, method, amount FROM payments) PIVOT (MAX(amount) FOR method IN ('card', 'cash'))
