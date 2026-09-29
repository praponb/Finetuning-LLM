SELECT category, stock_qty
FROM products
WHERE cost >= 460
UNION
SELECT category, stock_qty
FROM products
WHERE price < 690
ORDER BY category;
