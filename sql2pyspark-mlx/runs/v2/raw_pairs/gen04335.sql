SELECT status, customer_id FROM accounts WHERE balance < 46000 UNION ALL SELECT status, customer_id FROM accounts WHERE customer_id IN (14, 15, 16, 26);
