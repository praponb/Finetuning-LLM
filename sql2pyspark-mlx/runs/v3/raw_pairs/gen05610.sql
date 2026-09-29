SELECT
    id,
    brand,
    price,
    FLOOR(cost) cost_whole
FROM products
WHERE stock_qty BETWEEN 151 AND 152 OR SIZE(tags) >= 2
ORDER BY 3
