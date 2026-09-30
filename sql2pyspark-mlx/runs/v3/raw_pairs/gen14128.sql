SELECT YEAR(c.signup_date) AS signup_year, MAX(s.monthly_fee) AS max_monthly_fee FROM subscriptions s INNER JOIN customers c ON c.id = s.customer_id GROUP BY YEAR(c.signup_date) ORDER BY signup_year;
