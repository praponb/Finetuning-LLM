SELECT *
FROM (
    SELECT city, MAX(opened_date) AS last_opened_date, ROUND(AVG(sqft), 2) AS avg_sqft
    FROM stores
    GROUP BY city
) AS sub
WHERE avg_sqft >= 68
ORDER BY city
LIMIT 10;
