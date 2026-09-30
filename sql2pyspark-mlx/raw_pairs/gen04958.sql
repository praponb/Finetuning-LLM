WITH Ranked AS (
    SELECT
        id,
        status,
        paid_at,
        order_id,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) AS rn
    FROM payments
    WHERE order_id IS NOT NULL
)
SELECT *
FROM Ranked
WHERE rn = 1
