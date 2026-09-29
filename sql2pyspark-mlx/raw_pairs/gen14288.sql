SELECT signup_month, consumer_credit_limit
FROM (
    SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, SUM(CASE WHEN segment = 'Consumer' THEN credit_limit ELSE 0 END) AS consumer_credit_limit
    FROM customers
    GROUP BY DATE_TRUNC('MONTH', signup_date)
) AS s
WHERE consumer_credit_limit > 35
