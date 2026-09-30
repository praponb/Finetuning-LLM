SELECT *
FROM (
    SELECT QUARTER(signup_date) AS signup_quarter, ROUND((100 * SUM(CASE WHEN city = 'New York' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_new_york
    FROM customers
    GROUP BY QUARTER(signup_date)
) AS s
WHERE pct_new_york >= 39;
