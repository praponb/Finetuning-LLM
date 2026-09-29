WITH ranked AS (
    SELECT
        id,
        order_id,
        amount,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) AS row_num
    FROM payments
)
SELECT id, order_id, amount
FROM ranked
WHERE row_num <= 5
ORDER BY id;
