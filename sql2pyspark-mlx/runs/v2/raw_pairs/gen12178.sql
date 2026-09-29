SELECT
    a.id,
    a.status,
    a.customer_id,
    a.account_type
FROM accounts a
WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_time >= '2024-11-27')
ORDER BY a.customer_id DESC, id
LIMIT 10;
