SELECT
    id,
    cost,
    stock_qty,
    launch_date
FROM products
WHERE cost >= (SELECT MAX(cost) FROM products)
ORDER BY cost DESC, stock_qty DESC, id
LIMIT 50;
