WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount, id) row_num
    FROM orders
    WHERE shipping_fee BETWEEN 33 AND 38
)
SELECT *
FROM numbered
WHERE row_num <= 3
ORDER BY status, amount;
