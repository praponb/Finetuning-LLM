SELECT id, method FROM payments WHERE HOUR(paid_at) BETWEEN 9 AND 14 EXCEPT SELECT id, method FROM payments WHERE method = 'cash' ORDER BY id;
