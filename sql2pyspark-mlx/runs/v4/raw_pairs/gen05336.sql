WITH AccountTotals AS (
    SELECT account_id, MIN(amount) AS min_amount, COUNT(*) AS row_count
    FROM transactions
    GROUP BY account_id
)
SELECT a.account_type, A.min_amount, A.row_count
FROM accounts a
INNER JOIN AccountTotals A ON a.id = A.account_id
WHERE A.row_count >= 3
