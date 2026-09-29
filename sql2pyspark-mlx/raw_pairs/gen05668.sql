WITH AccountTotals AS (
    SELECT account_id, MAX(amount) - MIN(amount) AS amount_range, AVG(amount) AS avg_amount
    FROM transactions
    WHERE txn_time >= '2023-05-01'
    GROUP BY account_id
)
SELECT
    a.id,
    a.balance,
    A.amount_range,
    A.avg_amount
FROM accounts a
INNER JOIN AccountTotals A ON a.id = A.account_id;
