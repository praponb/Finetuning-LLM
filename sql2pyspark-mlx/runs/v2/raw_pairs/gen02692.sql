WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY shipping_fee, id) AS rn
    FROM orders
    WHERE discount <= 0.28
)
SELECT *
FROM numbered
WHERE rn = 1
