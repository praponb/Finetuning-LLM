SELECT *
FROM (
    SELECT city, DATE_TRUNC('MONTH', signup_date) AS signup_month, COUNT(*) AS num_customers
    FROM customers
    WHERE segment IS NOT NULL
    GROUP BY city, DATE_TRUNC('MONTH', signup_date)
) AS agg
WHERE num_customers >= 8;
