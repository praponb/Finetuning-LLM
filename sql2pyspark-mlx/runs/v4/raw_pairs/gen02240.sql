SELECT id, signup_date, age, ROUND(AVG(age) OVER (ORDER BY signup_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS age_moving_avg_4 FROM customers ORDER BY signup_date DESC
