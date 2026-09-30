SELECT o.status, o.amount, c.country
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
WHERE c.age <> 54 AND o.order_date BETWEEN '2023-04-01' AND '2025-07-01'
