SELECT
    segment,
    QUARTER(signup_date) AS signup_quarter,
    SUM(CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END) AS singapore_count,
    MIN(signup_date) AS min_signup_date
FROM customers
WHERE name NOT LIKE '%FRANK%'
GROUP BY 1, 2
HAVING singapore_count < 39
ORDER BY min_signup_date DESC;
