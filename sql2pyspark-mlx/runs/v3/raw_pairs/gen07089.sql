SELECT s.city, MIN(o.order_date) AS min_order_date FROM orders o INNER JOIN stores s ON s.id = o.store_id LEFT OUTER JOIN customers c ON o.customer_id = c.id GROUP BY s.city;
