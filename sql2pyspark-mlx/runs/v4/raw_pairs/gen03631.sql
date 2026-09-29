SELECT status, customer_id FROM accounts WHERE customer_id IS NOT NULL INTERSECT SELECT status, customer_id FROM accounts WHERE status IS NOT NULL ORDER BY status
