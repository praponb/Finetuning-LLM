WITH TopPayments AS (
    SELECT
        id,
        method,
        amount,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS rnk
    FROM payments
    WHERE method IN ('cash', 'card')
)
SELECT *
FROM TopPayments
WHERE rnk <= 3;
