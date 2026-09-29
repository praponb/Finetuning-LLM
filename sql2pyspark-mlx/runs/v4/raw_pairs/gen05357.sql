WITH TransactionsByAccount AS (
    SELECT account_id, SUM(CASE WHEN txn_type = 'FEE' THEN 1 ELSE 0 END) AS fee_count
    FROM transactions
    WHERE txn_type <> 'DEBIT'
    GROUP BY account_id
)
SELECT
    a.id,
    a.customer_id,
    a.status,
    T.fee_count
FROM accounts a
INNER JOIN TransactionsByAccount T ON a.id = T.account_id
ORDER BY customer_id;
