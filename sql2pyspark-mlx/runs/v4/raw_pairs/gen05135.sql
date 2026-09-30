SELECT id, customer_id FROM accounts WHERE customer_id IN (16, 20, 29) AND customer_id IS NULL AND balance < 74000 ORDER BY id, customer_id DESC
