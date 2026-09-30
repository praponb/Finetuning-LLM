WITH RankedAccounts AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC, id) rnk
    FROM accounts
    WHERE opened_date <= '2025-10-23'
)
SELECT
    id,
    customer_id,
    opened_date,
    account_type
FROM RankedAccounts
WHERE rnk <= 3
ORDER BY opened_date
