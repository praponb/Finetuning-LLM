SELECT YEAR(signup_date) signup_year, SUM(credit_limit) total_credit_limit, SUM(SUM(credit_limit)) OVER (ORDER BY YEAR(signup_date)) AS cumulative_credit_limit
FROM customers
WHERE city <> 'London'
GROUP BY YEAR(signup_date)
ORDER BY 2 ASC;
