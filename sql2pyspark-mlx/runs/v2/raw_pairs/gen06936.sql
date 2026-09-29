SELECT o.discount, p.amount
FROM orders o
LEFT JOIN payments p ON o.id = p.order_id
WHERE p.status IN ('SUCCESS', 'REFUNDED', 'FAILED') AND o.discount < 0.37
