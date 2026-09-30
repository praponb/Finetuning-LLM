SELECT
    a.id,
    a.customer_id,
    t.fee_amount,
    t.total_amount
FROM accounts a
LEFT OUTER JOIN (SELECT account_id, SUM(CASE WHEN txn_type = 'FEE' THEN amount ELSE 0 END) AS fee_amount, SUM(amount) AS total_amount
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.total_amount > 14000
ORDER BY a.customer_id
