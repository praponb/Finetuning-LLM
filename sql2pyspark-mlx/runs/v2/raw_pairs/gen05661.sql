SELECT segment, signup_year, avg_credit_limit
FROM (
    SELECT segment, YEAR(signup_date) AS signup_year, AVG(credit_limit) AS avg_credit_limit
    FROM customers
    GROUP BY segment, YEAR(signup_date)
) AS agg
WHERE avg_credit_limit < 15500
ORDER BY signup_year, segment DESC
