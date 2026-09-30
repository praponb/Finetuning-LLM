SELECT id, status, paid_at, order_id, FIRST_VALUE(order_id) OVER (PARTITION BY status ORDER BY paid_at, id) first_order_id FROM payments ORDER BY first_order_id DESC;
