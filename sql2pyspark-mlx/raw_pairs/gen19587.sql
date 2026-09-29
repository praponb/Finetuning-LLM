SELECT *
FROM payments
WHERE TO_DATE(paid_at) >= '2023-12-01'
ORDER BY amount ASC, id ASC
LIMIT 3
