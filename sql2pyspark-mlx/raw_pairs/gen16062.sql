SELECT a.opened_date, t.txn_time
FROM accounts a
INNER JOIN transactions t ON t.account_id = a.id
WHERE t.merchant IN ('Shell', 'Amazon', 'Lazada') AND a.account_type = 'CREDIT'
