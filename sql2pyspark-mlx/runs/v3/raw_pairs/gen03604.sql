SELECT p.status, o.order_date
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
ORDER BY o.order_date;
