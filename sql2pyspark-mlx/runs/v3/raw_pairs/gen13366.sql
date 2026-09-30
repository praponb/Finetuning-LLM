WITH src AS (
    SELECT *
    FROM accounts
    WHERE account_type <> 'SAVINGS'
),
summary AS (
    SELECT
        status,
        CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END AS opened_cohort,
        COUNT(*) AS total_count,
        COUNT(DISTINCT balance) AS unique_statuss
    FROM src
    GROUP BY status, CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT status, total_count, unique_statuss
FROM summary
WHERE unique_statuss > 4
