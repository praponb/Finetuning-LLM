SELECT id, tags, stock_qty + price AS stock_qty_plus_price
FROM products
WHERE price BETWEEN 560 AND 780
ORDER BY stock_qty_plus_price;
