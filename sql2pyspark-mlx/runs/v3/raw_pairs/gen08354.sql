SELECT
    t.id,
    t.txn_type,
    t.txn_time,
    a.customer_id,
    a.opened_date
FROM transactions t
INNER JOIN accounts a ON a.id = t.account_id
WHERE t.merchant IN ('Grab', 'Shell')
ORDER BY t.id DESC;
