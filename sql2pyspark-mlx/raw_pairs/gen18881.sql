SELECT city, opened_month, avg_sqft
FROM (
    SELECT city, DATE_TRUNC('MONTH', opened_date) AS opened_month, ROUND(AVG(sqft), 2) AS avg_sqft
    FROM stores
    WHERE sqft <= 6456
    GROUP BY city, DATE_TRUNC('MONTH', opened_date)
) AS sub
WHERE avg_sqft >= 73
ORDER BY avg_sqft, opened_month DESC, city
LIMIT 5
