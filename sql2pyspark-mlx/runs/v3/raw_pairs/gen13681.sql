SELECT id, method, paid_at, TO_DATE(paid_at) AS paid_at_date FROM payments WHERE NOT (paid_at >= '2023-11-17' AND (status IN ('SUCCESS', 'REFUNDED', 'FAILED') OR paid_at >= '2025-03-17'));
