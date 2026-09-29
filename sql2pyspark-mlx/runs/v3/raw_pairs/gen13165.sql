SELECT id, stock_qty
FROM products
WHERE stock_qty < (SELECT MIN(stock_qty) FROM products WHERE product_name IS NULL)
ORDER BY stock_qty DESC, id DESC;
