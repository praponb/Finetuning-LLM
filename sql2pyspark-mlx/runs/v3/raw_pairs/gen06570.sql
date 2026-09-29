SELECT a.status, a.opened_date, t.txn_time
FROM accounts a
INNER JOIN transactions t ON t.account_id = a.id
WHERE a.account_type IN ('SAVINGS', 'LOAN', 'CHECKING');
