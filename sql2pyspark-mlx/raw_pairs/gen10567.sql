SELECT p.id, p.method, p.paid_at, o.shipping_fee FROM payments p INNER JOIN orders o ON p.order_id = o.id WHERE HOUR(p.paid_at) BETWEEN 10 AND 14 ORDER BY method DESC, id ASC LIMIT 3
