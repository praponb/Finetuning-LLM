SELECT
    MONTH(signup_date) AS signup_month,
    SUM(age) AS total_age,
    COUNT(*) AS cnt,
    RANK() OVER (ORDER BY SUM(age) DESC) AS age_rank
FROM customers
GROUP BY MONTH(signup_date)
ORDER BY cnt DESC, signup_month
LIMIT 20
