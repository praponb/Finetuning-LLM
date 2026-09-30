WITH TransactionsByAccount AS (
    SELECT account_id, ROUND(AVG(amount), 2) AS avg_amount, SUM(amount) AS total_amount
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.opened_date,
    a.customer_id,
    T.avg_amount,
    T.total_amount
FROM accounts a
INNER JOIN TransactionsByAccount T ON a.id = T.account_id;
