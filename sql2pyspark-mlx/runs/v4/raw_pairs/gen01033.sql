SELECT id, method, paid_at, amount FROM payments WHERE method IN ('cash', 'bank_transfer', 'promptpay') AND amount > 1170 AND TO_DATE(paid_at) < '2024-10-01' ORDER BY paid_at, id LIMIT 10
