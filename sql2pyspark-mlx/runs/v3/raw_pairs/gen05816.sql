SELECT o.customer_id, t.min_amount
FROM orders o
LEFT OUTER JOIN (SELECT order_id, MIN(amount) min_amount
    FROM payments
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.min_amount >= 1510
ORDER BY customer_id DESC
