SELECT
    t.id,
    t.is_flagged,
    t.amount,
    t.txn_type,
    a.customer_id,
    DATEDIFF(CURRENT_DATE(), a.opened_date) AS days_since_opened_date
FROM transactions t
RIGHT JOIN accounts a ON a.id = t.account_id
