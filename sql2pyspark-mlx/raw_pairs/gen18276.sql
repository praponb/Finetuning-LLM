SELECT
    id,
    country,
    city,
    signup_date,
    age,
    ROUND(AVG(age) OVER (PARTITION BY country, city ORDER BY signup_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS age_moving_avg_3
FROM customers;
