SELECT
    DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month,
    region,
    QUARTER(opened_date) AS opened_quarter,
    MAX(sqft) AS max_sqft
FROM stores
WHERE city IN ('Tokyo', 'Berlin', 'New York') OR store_name LIKE '%Tow%'
GROUP BY 1, 2, 3
