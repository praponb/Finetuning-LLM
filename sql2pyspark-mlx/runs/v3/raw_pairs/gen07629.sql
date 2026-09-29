SELECT method, paid_at, status FROM payments WHERE method NOT IN ('bank_transfer', 'promptpay') AND TO_DATE(paid_at) < '2025-05-01' ORDER BY method;
