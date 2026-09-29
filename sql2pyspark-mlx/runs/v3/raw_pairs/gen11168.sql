SELECT *
FROM (
    SELECT is_active, ROUND((100 * SUM(CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_singapore, MIN(signup_date) AS min_signup_date
    FROM customers
    WHERE email IS NOT NULL
    GROUP BY is_active
) AS sub
WHERE pct_singapore < 33
ORDER BY min_signup_date;
