SELECT
    a.id,
    a.status,
    COUNT(t.id) transaction_count,
    MAX(t.amount) max_amount,
    MIN(t.txn_time) min_txn_time
FROM accounts a
LEFT OUTER JOIN transactions t ON a.id = t.account_id
WHERE YEAR(a.opened_date) >= 2025
GROUP BY a.id, a.status
HAVING MAX(t.amount) >= 2900
ORDER BY status;
