SELECT id, brand, price
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price DESC, id) AS rn
    FROM products
) AS r
WHERE rn <= 5
ORDER BY price DESC;
