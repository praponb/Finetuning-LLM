SELECT c.id, c.city, t.unique_customers
FROM customers c
INNER JOIN (SELECT customer_id, COUNT(DISTINCT customer_id) AS unique_customers
    FROM sessions
    GROUP BY customer_id) t ON c.id = t.customer_id;
