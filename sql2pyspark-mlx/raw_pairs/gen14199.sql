SELECT o.id, o.order_date, o.store_id FROM orders o WHERE o.id IN (SELECT order_id FROM payments WHERE method IN ('card', 'bank_transfer'));
