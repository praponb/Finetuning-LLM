SELECT order_date
FROM orders
INTERSECT
SELECT launch_date
FROM products;
