WITH TopPayments AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS rn FROM payments) SELECT id, status, amount, paid_at FROM TopPayments WHERE rn <= 5 ORDER BY id;
