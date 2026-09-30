SELECT p.status, o.store_id, o.discount, UPPER(p.method) AS method_upper FROM payments p LEFT JOIN orders o ON o.id = p.order_id;
