SELECT status FROM accounts WHERE balance >= 72000 UNION SELECT status FROM accounts WHERE account_type <> 'LOAN'
