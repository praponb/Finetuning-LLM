WITH RecentAccounts AS (
    SELECT *
    FROM accounts
    WHERE status IN ('FROZEN', 'OPEN', 'CLOSED') AND MONTH(opened_date) IN (7, 8, 12) AND opened_date < '2024-03-01'
),
accounts_stats AS (
    SELECT customer_id, COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) AS frozen_count
    FROM RecentAccounts
    GROUP BY customer_id
)
SELECT *
FROM accounts_stats
ORDER BY frozen_count DESC;
