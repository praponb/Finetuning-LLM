WITH TransactionsByAccount AS (
    SELECT account_id, SUM(amount) AS total_amount, COUNT(amount) AS amount_count
    FROM transactions
    GROUP BY account_id
)
SELECT
    accounts.id,
    accounts.balance,
    TransactionsByAccount.total_amount,
    TransactionsByAccount.amount_count
FROM accounts
INNER JOIN TransactionsByAccount ON accounts.id = TransactionsByAccount.account_id
WHERE TransactionsByAccount.amount_count > 4
ORDER BY balance DESC;
