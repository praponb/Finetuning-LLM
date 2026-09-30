SELECT
    t.txn_type,
    t.txn_time,
    t.amount,
    a.account_type
FROM transactions t
INNER JOIN accounts a ON a.id = t.account_id
WHERE t.merchant IN ('Grab', 'Lazada');
