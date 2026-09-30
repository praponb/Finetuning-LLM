SELECT *, DATE_TRUNC('MONTH', signup_date) AS signup_date_period
FROM customers
WHERE segment IS NOT NULL AND (credit_limit < 6800 OR city IS NOT NULL);
