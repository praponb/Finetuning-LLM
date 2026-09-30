SELECT id, customer_id, account_type FROM accounts WHERE status IS NOT NULL EXCEPT SELECT id, customer_id, account_type FROM accounts WHERE customer_id IS NULL ORDER BY customer_id;
