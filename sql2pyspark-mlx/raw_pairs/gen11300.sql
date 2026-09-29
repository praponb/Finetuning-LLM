SELECT status
FROM orders
WHERE MONTH(order_date) IN (3, 6, 11)
INTERSECT
SELECT product_name
FROM products
ORDER BY status;
