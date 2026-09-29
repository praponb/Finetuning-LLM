SELECT
    DATE_TRUNC('MONTH', signup_date) AS signup_month,
    SUM(age) AS total_age,
    COUNT(*) AS cnt,
    RANK() OVER (ORDER BY SUM(age) DESC) AS age_rank
FROM customers
WHERE age = 34
GROUP BY DATE_TRUNC('MONTH', signup_date)
ORDER BY cnt DESC, total_age DESC;
