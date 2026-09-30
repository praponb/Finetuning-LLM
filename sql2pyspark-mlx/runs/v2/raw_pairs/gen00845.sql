SELECT s.id, s.order_id, s.cost, s.status, o.status AS order_status FROM shipments s LEFT OUTER JOIN orders o ON o.id = s.order_id
