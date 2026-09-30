SELECT id, order_id, paid_at
FROM (
    SELECT
        id,
        order_id,
        paid_at,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY paid_at) AS rn
    FROM payments
) AS t
WHERE rn = 1;
