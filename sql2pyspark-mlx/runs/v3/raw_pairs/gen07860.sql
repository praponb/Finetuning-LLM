SELECT
    QUARTER(launch_date) AS launch_quarter,
    SUM(price) AS total_price,
    COUNT(*) AS cnt,
    ROUND(SUM(price) * 100.0 / SUM(SUM(price)) OVER (), 2) AS pct_of_total
FROM products
GROUP BY QUARTER(launch_date)
ORDER BY pct_of_total DESC, launch_quarter DESC
