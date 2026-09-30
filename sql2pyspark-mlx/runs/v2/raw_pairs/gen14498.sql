WITH transactions_by_account AS (
    SELECT account_id, COUNT(*) total_count
    FROM transactions
    GROUP BY account_id
)
SELECT a.id, a.opened_date, tba.total_count
FROM accounts a
JOIN transactions_by_account tba ON a.id = tba.account_id
ORDER BY tba.total_count DESC, id
LIMIT 1;
