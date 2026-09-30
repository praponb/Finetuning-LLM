SELECT status FROM accounts WHERE status = 'CLOSED' INTERSECT SELECT status FROM payments ORDER BY status
