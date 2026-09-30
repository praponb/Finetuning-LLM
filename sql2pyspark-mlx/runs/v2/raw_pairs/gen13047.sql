SELECT brand, SUM(stock_qty) AS total_stock_qty
FROM products
GROUP BY brand
ORDER BY brand, total_stock_qty DESC;
