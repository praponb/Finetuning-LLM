SELECT p.id, p.status, p.amount FROM payments p WHERE p.amount = (SELECT MAX(p2.amount) FROM payments p2 WHERE p2.status = p.status) ORDER BY p.status
