SELECT account_type, balance, customer_id FROM accounts WHERE status NOT IN ('OPEN', 'FROZEN') ORDER BY account_type DESC
