WITH valid_customers AS (
    SELECT *
    FROM customers
    WHERE credit_limit >= 6500 AND credit_limit <= 4700
),
summary AS (
    SELECT country, MAX(age) - MIN(age) AS age_range
    FROM valid_customers
    GROUP BY country
)
SELECT country, age_range
FROM summary
ORDER BY age_range DESC;
