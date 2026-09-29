SELECT id, status, account_type FROM accounts WHERE balance <= 57000 AND account_type = 'LOAN' AND balance IS NULL;
