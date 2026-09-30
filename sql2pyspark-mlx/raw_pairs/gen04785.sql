WITH account_stats AS (
    SELECT account_id, MAX(amount) - MIN(amount) AS amount_range, SUM(amount) AS total_amount
    FROM transactions
    GROUP BY account_id
)
SELECT
    accounts.account_type,
    accounts.customer_id,
    account_stats.amount_range,
    account_stats.total_amount
FROM accounts
JOIN account_stats ON accounts.id = account_stats.account_id
WHERE account_stats.total_amount >= 21000;
