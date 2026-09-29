WITH transactions_by_account AS (
    SELECT account_id, MAX(amount) - MIN(amount) amount_range, ROUND((100 * SUM(CASE WHEN merchant = 'Starbucks' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_starbucks
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.account_type,
    a.balance,
    tba.amount_range,
    tba.pct_starbucks
FROM accounts a
INNER JOIN transactions_by_account tba ON a.id = tba.account_id
ORDER BY tba.amount_range ASC;
