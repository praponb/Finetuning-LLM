SELECT id, stock_qty, LEAST(price, stock_qty) AS least_price_stock_qty
FROM products
WHERE price BETWEEN 680 AND 900 AND brand IN ('Globex', 'Initech')
ORDER BY stock_qty
