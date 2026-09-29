SELECT amount, paid_at, method, TRIM(method) AS clean_method FROM payments WHERE status IN ('SUCCESS', 'FAILED') ORDER BY method DESC, paid_at
