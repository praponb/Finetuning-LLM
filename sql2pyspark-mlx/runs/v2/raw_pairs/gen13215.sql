WITH numbered AS (SELECT id, status, paid_at, DENSE_RANK() OVER (PARTITION BY status ORDER BY paid_at DESC) rn FROM payments) SELECT * FROM numbered WHERE rn = 1 ORDER BY id DESC, paid_at DESC;
