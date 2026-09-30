SELECT
    a.opened_date,
    a.customer_id,
    t.is_flagged,
    t.txn_time,
    t.merchant
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id;
