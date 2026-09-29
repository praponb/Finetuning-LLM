SELECT
    t.txn_type,
    a.status,
    a.balance,
    c.is_active,
    c.segment,
    CONCAT_WS('/', a.status, a.account_type) AS status_account_type_key
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
LEFT OUTER JOIN customers c ON a.customer_id = c.id
ORDER BY status DESC
