SELECT *
FROM (
    SELECT
        id,
        supplier_id,
        cost,
        price
    FROM products
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR measure IN (cost AS COST_VAL, price AS PRICE)
)
