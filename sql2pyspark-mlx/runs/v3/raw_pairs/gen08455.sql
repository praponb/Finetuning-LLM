WITH transactions_by_account AS (
    SELECT account_id, COUNT(*) AS total_count
    FROM transactions
    GROUP BY account_id
)
SELECT a.balance, a.customer_id, tba.total_count
FROM accounts a
INNER JOIN transactions_by_account tba ON a.id = tba.account_id
ORDER BY a.balance DESC;
