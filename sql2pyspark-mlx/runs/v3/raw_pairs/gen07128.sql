WITH TopPayments AS (
    SELECT
        id,
        method,
        paid_at,
        status,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at, id) AS pos
    FROM payments
)
SELECT
    id,
    method,
    paid_at,
    status
FROM TopPayments
WHERE pos <= 1;
