SELECT id, order_id, status FROM payments WHERE status = 'SUCCESS' INTERSECT SELECT id, order_id, status FROM payments WHERE amount < 1350;
