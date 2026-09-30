SELECT status FROM orders INTERSECT SELECT status FROM payments WHERE method <> 'cash'
