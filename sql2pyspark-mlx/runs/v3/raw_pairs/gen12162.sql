SELECT * FROM (SELECT method, status, amount FROM payments WHERE TO_DATE(paid_at) >= '2025-06-17')
PIVOT (MAX(amount) FOR status IN ('REFUNDED', 'FAILED'))
