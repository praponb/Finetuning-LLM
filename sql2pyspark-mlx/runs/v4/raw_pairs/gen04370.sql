WITH TransactionsByAccount AS (
    SELECT account_id, COUNT(*) AS num_transactions
    FROM transactions
    WHERE account_id = 27
    GROUP BY account_id
)
SELECT a.id, a.account_type, T.num_transactions
FROM accounts a
JOIN TransactionsByAccount T ON a.id = T.account_id;
