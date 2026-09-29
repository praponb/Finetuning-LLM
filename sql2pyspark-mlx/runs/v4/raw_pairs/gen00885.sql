SELECT id, status FROM payments INTERSECT SELECT id, status FROM payments WHERE status = 'REFUNDED';
