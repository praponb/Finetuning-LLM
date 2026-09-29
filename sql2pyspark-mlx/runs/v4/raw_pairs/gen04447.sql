SELECT c.city, MAX(o.discount) AS max_discount FROM orders o INNER JOIN customers c ON c.id = o.customer_id FULL OUTER JOIN stores s ON o.store_id = s.id GROUP BY c.city HAVING MAX(o.discount) > 0.26
