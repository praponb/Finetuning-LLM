SELECT
    accounts.id,
    accounts.opened_date,
    accounts.account_type,
    t.fee_count
FROM accounts
LEFT OUTER JOIN (SELECT account_id, COUNT(CASE WHEN txn_type = 'FEE' THEN 1 END) AS fee_count
    FROM transactions
    WHERE txn_type IN ('FEE', 'DEBIT', 'REFUND')
    GROUP BY account_id) t ON accounts.id = t.account_id
WHERE t.fee_count >= 10;
