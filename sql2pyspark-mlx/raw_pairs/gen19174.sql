WITH account_totals AS (
    SELECT account_id, COUNT(CASE WHEN merchant = 'Amazon' THEN 1 END) AS amazon_count
    FROM transactions
    WHERE is_flagged = FALSE
    GROUP BY account_id
)
SELECT
    accounts.id,
    accounts.customer_id,
    accounts.status,
    account_totals.amazon_count
FROM accounts
INNER JOIN account_totals ON accounts.id = account_totals.account_id
ORDER BY customer_id;
