SELECT opened_date, customer_id FROM accounts WHERE customer_id IS NOT NULL EXCEPT SELECT opened_date, customer_id FROM accounts WHERE status IS NOT NULL
