SELECT
    a.opened_date,
    t.amount,
    t.txn_time,
    t.is_flagged
FROM accounts a
INNER JOIN transactions t ON t.account_id = a.id
WHERE t.is_flagged = FALSE
