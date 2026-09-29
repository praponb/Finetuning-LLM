SELECT
    id,
    category,
    price,
    supplier_id
FROM (
    SELECT
        id,
        category,
        price,
        supplier_id,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC, id) AS pos
    FROM products
) AS ranked
WHERE pos <= 3;
