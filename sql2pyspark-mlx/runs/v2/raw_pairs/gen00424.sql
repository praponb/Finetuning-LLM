SELECT
    c.name,
    c.signup_date,
    t.avg_discount,
    t.last_order_date
FROM customers c
INNER JOIN (SELECT customer_id, AVG(discount) AS avg_discount, MAX(order_date) AS last_order_date
    FROM orders
    WHERE store_id IS NOT NULL
    GROUP BY customer_id) t ON c.id = t.customer_id
WHERE t.avg_discount >= 0.38
