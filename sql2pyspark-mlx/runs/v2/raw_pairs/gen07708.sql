WITH valid_customers AS (
    SELECT *
    FROM customers
    WHERE country = 'SG' AND country <> 'US'
),
summary AS (
    SELECT QUARTER(signup_date) signup_quarter, MONTH(signup_date) signup_month, MAX(credit_limit) - MIN(credit_limit) credit_limit_range
    FROM valid_customers
    GROUP BY QUARTER(signup_date), MONTH(signup_date)
)
SELECT *
FROM summary
ORDER BY signup_quarter DESC, signup_month ASC
LIMIT 3;
