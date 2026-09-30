SELECT brand, ROUND(AVG(stock_qty), 2) avg_stock_qty, MIN(stock_qty) min_stock_qty
FROM products
WHERE supplier_id = 23
GROUP BY brand
HAVING ROUND(AVG(stock_qty), 2) < 60
