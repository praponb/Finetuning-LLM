SELECT *
FROM (
    SELECT
        id,
        brand,
        cost,
        price
    FROM products
) src
UNPIVOT (
    reading FOR metric IN (cost, price)
)
