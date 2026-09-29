SELECT
    id,
    supplier_id,
    measure,
    reading
FROM (
    SELECT
        id,
        supplier_id,
        cost,
        price
    FROM products
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (cost AS COST, price)
)
