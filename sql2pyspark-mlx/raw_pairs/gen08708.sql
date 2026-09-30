SELECT id, paid_at, amount, TO_DATE(paid_at) AS paid_at_date, UPPER(method) AS method_upper FROM payments WHERE status IN ('SUCCESS', 'REFUNDED', 'FAILED')
