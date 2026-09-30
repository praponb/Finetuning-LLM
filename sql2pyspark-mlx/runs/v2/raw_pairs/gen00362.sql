SELECT * FROM (SELECT DATE_TRUNC('MONTH', paid_at) AS paid_month, status, amount FROM payments)
PIVOT (AVG(amount) FOR status IN ('FAILED', 'SUCCESS', 'REFUNDED'))
