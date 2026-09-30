SELECT status FROM orders WHERE amount <= 1320 INTERSECT SELECT status FROM payments WHERE method <> 'card' ORDER BY status DESC;
