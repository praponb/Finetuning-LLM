SELECT o.customer_id, p.amount
FROM orders o
LEFT JOIN payments p ON o.id = p.order_id
WHERE p.method IS NOT NULL;
