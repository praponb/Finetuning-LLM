SELECT launch_date FROM products WHERE stock_qty BETWEEN 202 AND 349 INTERSECT SELECT order_date FROM orders ORDER BY launch_date
