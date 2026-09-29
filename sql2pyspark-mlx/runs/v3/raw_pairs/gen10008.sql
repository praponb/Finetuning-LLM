SELECT
    country,
    segment,
    MIN(signup_date) AS min_signup_date,
    SUM(CASE WHEN city = 'London' THEN age ELSE 0 END) AS london_age
FROM customers
GROUP BY country, segment
HAVING london_age >= 78;
