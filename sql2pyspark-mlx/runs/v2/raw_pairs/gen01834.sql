SELECT id, account_type FROM accounts WHERE status <> 'FROZEN' AND account_type IN ('CREDIT', 'CHECKING', 'LOAN') ORDER BY account_type, id DESC
