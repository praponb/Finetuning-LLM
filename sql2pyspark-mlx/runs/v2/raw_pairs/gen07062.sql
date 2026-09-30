WITH TopPayments AS (
    SELECT
        id,
        order_id,
        paid_at,
        amount,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY paid_at DESC) AS row_num
    FROM payments
)
SELECT *
FROM TopPayments
WHERE row_num = 1
ORDER BY row_num DESC;
