SELECT
    id,
    brand,
    cost,
    price
FROM (
    SELECT
        id,
        brand,
        cost,
        price,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost DESC, id) pos
    FROM products
) t
WHERE pos = 1;
