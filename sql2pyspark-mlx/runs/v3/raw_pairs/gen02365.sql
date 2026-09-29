SELECT category, MAX(stock_qty) - MIN(stock_qty) AS stock_qty_range, MAX(cost) AS max_cost
FROM products
WHERE price <= 1060 AND brand IN ('Globex', 'Stark', 'Acme')
GROUP BY category
HAVING MAX(stock_qty) - MIN(stock_qty) < 43
ORDER BY category DESC, max_cost ASC
