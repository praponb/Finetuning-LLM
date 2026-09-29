SELECT o.store_id, ROUND(AVG(p.amount), 2) AS avg_amount
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
GROUP BY o.store_id;
