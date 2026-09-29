SELECT id, paid_at, method, status, INITCAP(TRIM(status)) AS status_title FROM payments
