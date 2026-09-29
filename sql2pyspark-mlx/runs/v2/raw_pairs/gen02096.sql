SELECT
    id,
    country,
    signup_date,
    age,
    ROUND(AVG(age) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS age_moving_avg_7
FROM customers
ORDER BY id;
