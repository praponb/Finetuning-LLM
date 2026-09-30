SELECT *
FROM (
    SELECT MONTH(opened_date) AS opened_month, city, ROUND(STDDEV(sqft), 2) AS stddev_sqft
    FROM stores
    GROUP BY MONTH(opened_date), city
) AS sub
WHERE stddev_sqft >= 78
ORDER BY city DESC, opened_month
LIMIT 5;
