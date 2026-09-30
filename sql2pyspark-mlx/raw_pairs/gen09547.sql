SELECT
    id,
    country,
    signup_date,
    city,
    LAST_VALUE(city) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_city
FROM customers
ORDER BY city
