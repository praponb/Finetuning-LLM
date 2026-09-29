SELECT * FROM (SELECT MONTH(paid_at) AS paid_month, status, amount FROM payments)
PIVOT (MAX(amount) FOR status IN ('REFUNDED', 'FAILED', 'SUCCESS'))
ORDER BY paid_month
