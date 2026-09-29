SELECT a.id, a.status, t.refund_count
FROM accounts a
INNER JOIN (SELECT account_id, SUM(CASE WHEN txn_type = 'REFUND' THEN 1 ELSE 0 END) AS refund_count
    FROM transactions
    WHERE is_flagged
    GROUP BY account_id) t ON a.id = t.account_id
ORDER BY id DESC, 2;
