SELECT *
FROM (
    SELECT country, COUNT(DISTINCT credit_limit) AS unique_citys, MAX(signup_date) AS last_signup_date
    FROM customers
    GROUP BY country
) AS s
WHERE unique_citys >= 4;
