WITH Ranked AS (
    SELECT
        id,
        order_id,
        paid_at,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id) AS pos
    FROM payments
    WHERE TO_DATE(paid_at) < '2023-05-01'
)
SELECT *
FROM Ranked
WHERE pos <= 3
ORDER BY id DESC, paid_at;
