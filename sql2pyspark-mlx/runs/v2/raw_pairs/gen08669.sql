SELECT
    region,
    QUARTER(opened_date) AS opened_quarter,
    MAX(sqft) AS max_sqft,
    SUM(sqft) AS total_sqft,
    MIN(opened_date) AS min_opened_date
FROM stores
GROUP BY 1, 2
HAVING max_sqft < 11700
ORDER BY region DESC;
