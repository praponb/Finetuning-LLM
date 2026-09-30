SELECT id, status, amount, SUM(amount) OVER (PARTITION BY status) AS status_sum_amount FROM payments WHERE method != 'card';
