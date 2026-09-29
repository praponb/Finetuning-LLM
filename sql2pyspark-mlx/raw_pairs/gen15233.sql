SELECT amount, method, paid_at, ROUND(amount, 0) AS amount_rounded FROM payments WHERE method = 'cash';
