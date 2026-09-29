SELECT customers.city, COUNT(orders.id) order_count FROM customers LEFT JOIN orders ON customers.id = orders.customer_id GROUP BY customers.city ORDER BY order_count DESC;
