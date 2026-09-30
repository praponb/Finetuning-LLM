SELECT id, paid_at
FROM payments
WHERE TO_DATE(paid_at) >= '2024-06-01';
