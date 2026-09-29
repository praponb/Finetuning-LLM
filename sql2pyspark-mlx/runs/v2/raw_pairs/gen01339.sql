SELECT category, stock_qty
FROM products
WHERE price < 920
UNION
SELECT category, stock_qty
FROM products
WHERE SIZE(tags) >= 1
ORDER BY stock_qty DESC;
