SELECT id, paid_at, TRIM(method) AS clean_method, DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_at_label FROM payments ORDER BY paid_at_label
