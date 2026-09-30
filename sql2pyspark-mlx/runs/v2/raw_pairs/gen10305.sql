SELECT
    t.id,
    t.account_id,
    t.merchant,
    t.txn_time,
    a.status,
    a.opened_date
FROM transactions t
FULL OUTER JOIN accounts a ON a.id = t.account_id;
