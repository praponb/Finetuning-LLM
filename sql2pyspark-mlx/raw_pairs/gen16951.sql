SELECT * FROM (SELECT method, status, amount FROM payments WHERE amount IS NULL)
PIVOT (AVG(amount) FOR status IN ('REFUNDED', 'FAILED'));
