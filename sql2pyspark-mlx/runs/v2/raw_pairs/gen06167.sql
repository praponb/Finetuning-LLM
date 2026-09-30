SELECT
    t.id,
    t.txn_type,
    t.amount,
    t.txn_time,
    a.opened_date,
    DATE_TRUNC('YEAR', t.txn_time) AS txn_time_period
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
