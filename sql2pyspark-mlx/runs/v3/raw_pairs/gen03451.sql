SELECT s.id, s.status, s.order_id, s.cost, o.store_id, o.amount FROM shipments s FULL OUTER JOIN orders o ON o.id = s.order_id WHERE s.cost < 55 ORDER BY s.id DESC, amount DESC;
