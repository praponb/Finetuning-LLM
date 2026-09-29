SELECT
    id,
    credit_limit,
    LAST_DAY(signup_date) AS signup_date_month_end,
    LENGTH(city) AS city_length
FROM customers
WHERE age > 63
