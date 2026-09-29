SELECT
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    PERCENTILE_APPROX(price, 0.9) AS price_p90,
    COUNT(*) AS cnt,
    COUNT(*) AS event_count
FROM products
GROUP BY 1
