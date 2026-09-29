SELECT customers.segment, MIN(orders.order_date) AS first_order_date
FROM orders
LEFT JOIN customers ON orders.customer_id = customers.id
JOIN stores ON stores.id = orders.store_id
GROUP BY customers.segment
