SELECT QUARTER(signup_date) AS signup_quarter, country, SUM(credit_limit) AS total_credit_limit FROM customers GROUP BY 1, 2 ORDER BY country DESC
