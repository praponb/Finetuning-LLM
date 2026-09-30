WITH RecentAccounts AS (
    SELECT *
    FROM accounts
    WHERE status IN ('FROZEN', 'OPEN', 'CLOSED') AND MONTH(opened_date) IN (7, 8, 12) AND opened_date < '2024-03-01'
),
status_summary AS (
    SELECT status, COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) AS frozen_count, MAX(balance) - MIN(balance) AS balance_range
    FROM RecentAccounts
    GROUP BY status
)
SELECT status, frozen_count, balance_range
FROM status_summary
WHERE balance_range > 77
ORDER BY frozen_count DESC;
