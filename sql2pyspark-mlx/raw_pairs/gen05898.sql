WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id) AS row_num
    FROM payments
)
SELECT *
FROM ranked
WHERE row_num <= 3
ORDER BY paid_at DESC, id
