SELECT
    country,
    signup_year,
    stddev_credit_limit,
    unique_segments
FROM (
    SELECT
        country,
        YEAR(signup_date) AS signup_year,
        ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit,
        COUNT(DISTINCT age) AS unique_segments
    FROM customers
    WHERE credit_limit <= 14700
    GROUP BY country, YEAR(signup_date)
) AS sub
WHERE stddev_credit_limit < 66;
