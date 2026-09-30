SELECT YEAR(signup_date) AS signup_year, SUM(credit_limit) AS total_credit_limit FROM customers GROUP BY signup_year ORDER BY total_credit_limit DESC
