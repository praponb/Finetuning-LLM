SELECT *, SUBSTRING(status, 1, 3) AS status_prefix, COALESCE(amount, 0) AS amount_filled FROM payments WHERE status NOT IN ('SUCCESS', 'REFUNDED');
