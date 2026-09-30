SELECT status, customer_id FROM accounts WHERE customer_id IN (2, 10, 22) UNION ALL SELECT status, customer_id FROM accounts WHERE customer_id IS NOT NULL;
