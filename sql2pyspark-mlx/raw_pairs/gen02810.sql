WITH filtered_accounts AS (
    SELECT *
    FROM accounts
    WHERE opened_date BETWEEN '2024-12-01' AND '2025-10-03'
),
AccountsStats AS (
    SELECT status, customer_id, MAX(balance) AS max_balance
    FROM filtered_accounts
    GROUP BY status, customer_id
)
SELECT status, customer_id, max_balance
FROM AccountsStats
ORDER BY customer_id DESC, status
LIMIT 1;
