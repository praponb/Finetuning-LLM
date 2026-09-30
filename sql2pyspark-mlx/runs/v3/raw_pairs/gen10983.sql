SELECT o.customer_id, p.status, p.order_id
FROM orders o
LEFT OUTER JOIN payments p ON p.order_id = o.id
WHERE o.customer_id IS NULL
ORDER BY o.customer_id DESC;
