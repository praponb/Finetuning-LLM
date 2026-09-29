SELECT id, status FROM payments INTERSECT SELECT id, status FROM payments WHERE amount BETWEEN 810 AND 1310
