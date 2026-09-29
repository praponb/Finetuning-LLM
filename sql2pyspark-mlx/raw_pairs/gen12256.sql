SELECT id, brand, price
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price, id) AS rn
    FROM products
) AS t
WHERE rn <= 3
ORDER BY 1 DESC;
