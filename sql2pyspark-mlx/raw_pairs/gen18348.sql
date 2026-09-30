SELECT
    t.id,
    t.txn_type,
    a.balance,
    a.opened_date,
    INITCAP(TRIM(a.account_type)) account_type_title
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
ORDER BY 3 DESC, account_type_title DESC;
