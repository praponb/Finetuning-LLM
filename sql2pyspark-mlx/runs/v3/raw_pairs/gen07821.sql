SELECT id, account_type, opened_date, COALESCE(balance, 0) AS balance_filled FROM accounts WHERE status = 'FROZEN' AND (account_type = 'CREDIT' OR account_type = 'CHECKING') ORDER BY account_type
