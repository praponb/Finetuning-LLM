SELECT a.id, a.account_type, a.status FROM accounts a WHERE a.status IN ('FROZEN', 'OPEN', 'CLOSED') AND a.id IN (SELECT account_id FROM transactions) ORDER BY a.id DESC LIMIT 10
