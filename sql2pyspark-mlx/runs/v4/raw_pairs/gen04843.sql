SELECT o.status, o.discount, o.order_date, c.age, c.country, SUBSTRING(c.name, 1, 4) AS name_prefix FROM orders o INNER JOIN customers c ON o.customer_id = c.id ORDER BY o.discount DESC;
