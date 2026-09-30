SELECT a.balance, a.opened_date, t.txn_time
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
WHERE t.is_flagged
ORDER BY t.txn_time, opened_date DESC;
