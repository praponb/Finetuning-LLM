SELECT id, amount, method, paid_at FROM payments WHERE method IN ('promptpay', 'card', 'cash');
