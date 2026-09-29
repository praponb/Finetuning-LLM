SELECT
    a.id,
    a.balance,
    t.refund_amount,
    t.avg_amount
FROM accounts a
INNER JOIN (SELECT account_id, SUM(CASE WHEN txn_type = 'REFUND' THEN amount ELSE 0 END) AS refund_amount, ROUND(AVG(amount), 2) AS avg_amount
    FROM transactions
    WHERE txn_type = 'FEE'
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.refund_amount >= 39
ORDER BY avg_amount DESC, id
LIMIT 1;
