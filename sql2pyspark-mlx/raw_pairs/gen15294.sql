WITH ranked AS (
    SELECT
        id,
        channel,
        amount,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount, id) AS row_num
    FROM orders
    WHERE channel IN ('web', 'store', 'mobile')
)
SELECT id, channel, amount
FROM ranked
WHERE row_num <= 3
ORDER BY channel ASC;
