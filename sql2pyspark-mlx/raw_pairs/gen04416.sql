WITH TopPayments AS (SELECT *, DENSE_RANK() OVER (PARTITION BY method ORDER BY paid_at) AS rn FROM payments WHERE method IS NOT NULL) SELECT * FROM TopPayments WHERE rn = 1 ORDER BY amount DESC;
