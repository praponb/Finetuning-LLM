SELECT c.city, t.min_discount
FROM customers c
JOIN (SELECT customer_id, MIN(discount) AS min_discount
    FROM orders
    GROUP BY customer_id) t ON c.id = t.customer_id
ORDER BY city DESC
