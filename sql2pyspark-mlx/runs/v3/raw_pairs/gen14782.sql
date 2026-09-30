SELECT id, stock_qty
FROM products
WHERE stock_qty >= (SELECT AVG(stock_qty) FROM products WHERE cost BETWEEN 390 AND 520)
ORDER BY id DESC;
