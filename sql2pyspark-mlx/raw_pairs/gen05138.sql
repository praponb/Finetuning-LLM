SELECT *
FROM (
    SELECT QUARTER(opened_date) AS opened_quarter, AVG(sqft) AS avg_sqft, SUM(CASE WHEN region = 'East' THEN 1 ELSE 0 END) AS east_count
    FROM stores
    WHERE city NOT IN ('Berlin', 'New York')
    GROUP BY QUARTER(opened_date)
) AS t
WHERE avg_sqft >= 5600
