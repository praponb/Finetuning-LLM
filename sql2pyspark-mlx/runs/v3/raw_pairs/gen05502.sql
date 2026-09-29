WITH valid_customers AS (
    SELECT *, CASE WHEN segment IN ('Consumer', 'SMB') THEN 'Core' ELSE 'Other' END segment_group
    FROM customers
    WHERE country <> 'JP'
),
signup_quarter_summary AS (
    SELECT QUARTER(signup_date) signup_quarter, MAX(signup_date) last_signup_date
    FROM valid_customers
    GROUP BY QUARTER(signup_date)
)
SELECT signup_quarter, last_signup_date
FROM signup_quarter_summary
ORDER BY signup_quarter DESC
