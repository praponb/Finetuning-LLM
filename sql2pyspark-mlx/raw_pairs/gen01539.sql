WITH filtered_customers AS (
    SELECT *, SUBSTRING(segment, 1, 4) AS segment_prefix
    FROM customers
    WHERE is_active OR segment = 'SMB'
),
summary AS (
    SELECT YEAR(signup_date) AS signup_year, ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit
    FROM filtered_customers
    GROUP BY YEAR(signup_date)
)
SELECT *
FROM summary
WHERE stddev_credit_limit < 25;
