SELECT *
FROM (
    SELECT
        category,
        brand,
        stock_qty
    FROM products
) src
PIVOT (
    SUM(stock_qty) AS total,
    COUNT(1) AS cnt
    FOR brand IN ('Acme' AS acme, 'Umbrella' AS umbrella, 'Initech' AS initech)
)
