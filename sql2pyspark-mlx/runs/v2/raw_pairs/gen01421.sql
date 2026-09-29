WITH TransactionsByAccount AS (
    SELECT account_id, COUNT(CASE WHEN merchant = 'Grab' THEN 1 END) AS grab_count
    FROM transactions
    WHERE is_flagged
    GROUP BY account_id
)
SELECT a.id, a.balance, T.grab_count
FROM accounts a
INNER JOIN TransactionsByAccount T ON a.id = T.account_id
ORDER BY grab_count
