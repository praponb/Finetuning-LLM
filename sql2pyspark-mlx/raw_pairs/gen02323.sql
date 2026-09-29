SELECT id, status, opened_date, account_type FROM accounts WHERE NOT (account_type NOT IN ('CHECKING', 'CREDIT', 'LOAN') AND account_type IN ('CHECKING', 'CREDIT') AND customer_id IN (2, 30))
