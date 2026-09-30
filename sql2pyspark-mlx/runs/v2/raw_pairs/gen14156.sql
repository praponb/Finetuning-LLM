SELECT id, amount, status FROM payments WHERE TO_DATE(paid_at) >= '2023-09-01' ORDER BY status;
