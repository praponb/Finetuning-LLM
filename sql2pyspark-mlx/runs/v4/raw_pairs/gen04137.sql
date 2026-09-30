SELECT id, amount, status, method FROM payments WHERE (amount > 610 AND status IN ('SUCCESS', 'FAILED')) OR TO_DATE(paid_at) >= '2023-12-07'
