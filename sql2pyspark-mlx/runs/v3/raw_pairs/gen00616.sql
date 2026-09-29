SELECT
    DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month,
    city,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values,
    SUM(sqft) AS total_sqft
FROM stores
GROUP BY 1, 2
HAVING total_sqft >= 92000
ORDER BY opened_month
