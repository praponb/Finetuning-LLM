SELECT
    a.id,
    a.balance,
    a.account_type,
    t.credit_count,
    t.avg_amount
FROM accounts a
LEFT JOIN (SELECT account_id, SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END) AS credit_count, ROUND(AVG(amount), 2) AS avg_amount
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
