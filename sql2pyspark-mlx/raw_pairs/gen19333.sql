SELECT account_type FROM accounts INTERSECT ALL SELECT account_type FROM accounts WHERE account_type IN ('CREDIT', 'SAVINGS', 'CHECKING')
