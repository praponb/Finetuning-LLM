SELECT order_items.product_id, orders.discount, customers.country FROM order_items LEFT JOIN orders ON orders.id = order_items.order_id JOIN customers ON orders.customer_id = customers.id;
