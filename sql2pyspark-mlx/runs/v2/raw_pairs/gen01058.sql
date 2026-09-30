SELECT id, status, paid_at, NTILE(10) OVER (PARTITION BY status ORDER BY paid_at, id) AS bucket FROM payments ORDER BY id DESC;
