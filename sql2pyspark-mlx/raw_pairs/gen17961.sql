SELECT opened_date FROM accounts WHERE account_type IN ('CREDIT', 'LOAN', 'SAVINGS') EXCEPT SELECT opened_date FROM accounts WHERE status = 'CLOSED';
