WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price DESC, id) AS rn
    FROM products
)
SELECT id, brand, price
FROM numbered
WHERE rn <= 1
ORDER BY id DESC;
