WITH cleaned AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        CASE WHEN amount >= 1470 THEN 'High' WHEN amount >= 670 THEN 'Medium' ELSE 'Low' END AS amount_level,
        COALESCE(amount, 0) AS amount_filled
    FROM payments
    WHERE paid_at > TO_DATE('2023-11-16')
      AND order_id IS NOT NULL
)
SELECT id
FROM cleaned
EXCEPT
SELECT id
FROM payments
WHERE TO_DATE(paid_at) < '2024-07-13'
