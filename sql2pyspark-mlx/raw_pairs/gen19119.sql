WITH ValidAccounts AS (
    SELECT *
    FROM accounts
    WHERE opened_date <= '2024-03-01' AND (opened_date <= '2025-08-01' OR status IN ('OPEN', 'FROZEN'))
),
agg AS (
    SELECT MONTH(opened_date) AS opened_month, MAX(opened_date) AS last_opened_date
    FROM ValidAccounts
    GROUP BY MONTH(opened_date)
)
SELECT opened_month, last_opened_date
FROM agg
