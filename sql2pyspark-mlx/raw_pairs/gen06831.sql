SELECT
    id,
    city,
    opened_date,
    sqft,
    ROUND(AVG(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS sqft_moving_avg_7,
    ROUND(sqft / SUM(sqft) OVER (PARTITION BY YEAR(opened_date)), 3) AS sqft_share
FROM stores
WHERE store_name LIKE '%Old%';
