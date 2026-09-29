WITH ranked_payments AS (SELECT id, status, paid_at, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) rn FROM payments) SELECT id, status, paid_at FROM ranked_payments WHERE rn <= 1
