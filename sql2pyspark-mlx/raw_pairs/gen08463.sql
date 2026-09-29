WITH recent_accounts AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), opened_date) / 365.25 AS years_since_opened_date
    FROM accounts
    WHERE balance IS NOT NULL
),
summary AS (
    SELECT status, COUNT(DISTINCT years_since_opened_date) AS unique_statuss, AVG(years_since_opened_date) AS avg_years_since_opened_date
    FROM recent_accounts
    GROUP BY status
)
SELECT *
FROM summary
ORDER BY avg_years_since_opened_date, status
LIMIT 3
