WITH transactions_by_account AS (
    SELECT account_id, COUNT(amount) AS amount_count
    FROM transactions
    GROUP BY account_id
)
SELECT accounts.opened_date, transactions_by_account.amount_count
FROM accounts
INNER JOIN transactions_by_account ON accounts.id = transactions_by_account.account_id
WHERE transactions_by_account.amount_count > 9
ORDER BY opened_date
