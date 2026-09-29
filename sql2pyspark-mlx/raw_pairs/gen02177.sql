SELECT o.order_date, p.paid_at
FROM orders o
LEFT JOIN payments p ON o.id = p.order_id
ORDER BY o.order_date DESC;
