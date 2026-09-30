SELECT s.city, MIN(o.order_date) AS first_order_date FROM orders o LEFT OUTER JOIN customers c ON c.id = o.customer_id INNER JOIN stores s ON s.id = o.store_id GROUP BY s.city;
