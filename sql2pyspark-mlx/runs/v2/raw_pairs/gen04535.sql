SELECT signup_month, min_credit_limit
FROM (
    SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, MIN(credit_limit) AS min_credit_limit
    FROM customers
    GROUP BY DATE_TRUNC('MONTH', signup_date)
) AS s
WHERE min_credit_limit > 15500
