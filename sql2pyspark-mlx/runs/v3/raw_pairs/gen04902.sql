WITH account_summary AS (
    SELECT account_id, ROUND(AVG(amount), 2) avg_amount
    FROM transactions
    GROUP BY account_id
)
SELECT accounts.customer_id, accounts.opened_date, account_summary.avg_amount
FROM accounts
LEFT JOIN account_summary ON accounts.id = account_summary.account_id
ORDER BY customer_id;
