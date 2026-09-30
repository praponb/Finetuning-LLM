SELECT a.id, a.status FROM accounts a WHERE a.account_type = 'LOAN' AND a.id IN (SELECT account_id FROM transactions) ORDER BY a.status
