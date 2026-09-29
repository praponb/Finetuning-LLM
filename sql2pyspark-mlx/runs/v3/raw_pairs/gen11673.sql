SELECT
    t.id,
    t.amount,
    a.opened_date,
    a.status,
    a.customer_id,
    CASE WHEN t.txn_type IN ('CREDIT', 'DEBIT') THEN 'Tier 1' ELSE 'Other' END AS txn_type_group
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE t.amount BETWEEN 1600 AND 3200
