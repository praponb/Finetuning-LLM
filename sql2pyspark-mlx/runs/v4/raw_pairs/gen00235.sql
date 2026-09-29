SELECT *
FROM (
    SELECT YEAR(opened_date) AS opened_year, AVG(sqft) AS avg_sqft, MAX(sqft) AS max_sqft
    FROM stores
    WHERE region NOT IN ('West', 'South')
    GROUP BY YEAR(opened_date)
) AS sub
WHERE avg_sqft > 8400
ORDER BY avg_sqft ASC;
