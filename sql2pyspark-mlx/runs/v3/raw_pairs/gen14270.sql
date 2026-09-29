SELECT id, launch_date, stock_qty
FROM products
WHERE launch_date = (SELECT MAX(launch_date) FROM products)
ORDER BY stock_qty DESC, id
LIMIT 20
