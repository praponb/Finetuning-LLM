WITH filtered_customers AS (
    SELECT *, TRIM(name) AS clean_name
    FROM customers
    WHERE segment <> 'SMB'
),
summary AS (
    SELECT is_active, SUM(age) AS total_age, SUM(credit_limit) AS total_credit_limit
    FROM filtered_customers
    GROUP BY is_active
)
SELECT is_active, total_credit_limit
FROM summary
ORDER BY total_credit_limit DESC;
