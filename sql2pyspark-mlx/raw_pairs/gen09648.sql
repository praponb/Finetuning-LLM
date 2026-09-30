WITH AccountStats AS (
    SELECT account_id, SUM(CASE WHEN txn_type = 'REFUND' THEN 1 ELSE 0 END) refund_count
    FROM transactions
    GROUP BY account_id
)
SELECT a.id, a.opened_date, A.refund_count
FROM accounts a
LEFT JOIN AccountStats A ON a.id = A.account_id
ORDER BY A.refund_count;
