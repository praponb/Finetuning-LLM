WITH account_totals AS (
    SELECT account_id, ROUND(AVG(amount), 2) AS avg_amount
    FROM transactions
    WHERE merchant = 'Amazon'
    GROUP BY account_id
)
SELECT
    accounts.id,
    accounts.balance,
    accounts.account_type,
    account_totals.avg_amount
FROM accounts
LEFT OUTER JOIN account_totals ON accounts.id = account_totals.account_id
WHERE account_totals.avg_amount >= 45
ORDER BY id;
