SELECT product_name, 'product' AS source FROM products UNION SELECT channel, 'order' AS source FROM orders WHERE status != 'SHIPPED';
