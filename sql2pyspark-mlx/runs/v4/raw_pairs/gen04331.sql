SELECT
    t.merchant,
    t.is_flagged,
    a.balance,
    a.account_type
FROM transactions t
FULL OUTER JOIN accounts a ON a.id = t.account_id
WHERE t.txn_type = 'FEE'
ORDER BY a.balance;
