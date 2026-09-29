SELECT *
FROM (
    SELECT segment, DATE_TRUNC('MONTH', signup_date) AS signup_month, SUM(CASE WHEN city = 'London' THEN credit_limit ELSE 0 END) AS london_credit_limit
    FROM customers
    WHERE name IS NOT NULL
    GROUP BY segment, DATE_TRUNC('MONTH', signup_date)
) AS t
WHERE london_credit_limit >= 37;
