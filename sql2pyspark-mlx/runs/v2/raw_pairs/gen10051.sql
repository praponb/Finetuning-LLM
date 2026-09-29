SELECT
    t.id,
    t.merchant,
    t.txn_time,
    t.is_flagged,
    a.account_type,
    CASE WHEN a.account_type IN ('LOAN', 'SAVINGS') THEN 'Core' ELSE 'Other' END AS account_type_group
FROM transactions t
RIGHT JOIN accounts a ON a.id = t.account_id
WHERE t.txn_time < '2023-10-01'
ORDER BY txn_time, account_type DESC
