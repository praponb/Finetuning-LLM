SELECT t.id, t.txn_type, t.merchant, a.status, a.opened_date, a.account_type FROM transactions t LEFT OUTER JOIN accounts a ON t.account_id = a.id WHERE t.account_id = 21 ORDER BY status DESC;
