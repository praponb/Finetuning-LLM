WITH customer_summary AS (
    SELECT customer_id, COUNT(*) AS num_orders
    FROM orders
    GROUP BY customer_id
)
SELECT c.id, c.city, cs.num_orders
FROM customers c
INNER JOIN customer_summary cs ON c.id = cs.customer_id
WHERE cs.num_orders >= 4
ORDER BY id DESC, city;
