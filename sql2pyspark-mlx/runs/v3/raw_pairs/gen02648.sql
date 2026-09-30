SELECT QUARTER(paid_at) AS paid_quarter, COUNT(DISTINCT amount) AS unique_methods FROM payments WHERE status <> 'REFUNDED' AND amount <= 1010 GROUP BY 1
