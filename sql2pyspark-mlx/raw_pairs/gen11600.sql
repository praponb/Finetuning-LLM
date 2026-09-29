SELECT p.id, p.status, p.order_id, p.method, o.order_date, o.discount, o.status AS order_status FROM payments p INNER JOIN orders o ON p.order_id = o.id WHERE TO_DATE(p.paid_at) < '2023-04-01';
