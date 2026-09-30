SELECT account_type, balance, customer_id FROM accounts WHERE account_type NOT IN ('LOAN', 'CHECKING', 'CREDIT') ORDER BY account_type
