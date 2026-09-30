SELECT * FROM (SELECT TO_DATE(paid_at) AS paid_day, status, amount FROM payments WHERE method IN ('bank_transfer', 'card', 'paypal'))
PIVOT (AVG(amount) FOR status IN ('SUCCESS', 'FAILED', 'REFUNDED'))
ORDER BY paid_day
