WITH TopPayments AS (SELECT id, status, paid_at, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) AS rnk FROM payments) SELECT * FROM TopPayments WHERE rnk <= 1 ORDER BY paid_at ASC;
