SELECT customer_id FROM accounts WHERE balance IS NOT NULL UNION SELECT customer_id FROM accounts WHERE status NOT IN ('CLOSED', 'FROZEN')
