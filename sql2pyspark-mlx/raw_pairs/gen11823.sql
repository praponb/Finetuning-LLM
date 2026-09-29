SELECT p.status, p.amount, o.order_date, LENGTH(p.status) AS status_length FROM payments p LEFT JOIN orders o ON o.id = p.order_id
