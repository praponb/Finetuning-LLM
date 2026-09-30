SELECT opened_month, min_sqft, stddev_sqft
FROM (
    SELECT DATE_TRUNC('MONTH', opened_date) opened_month, MIN(sqft) min_sqft, ROUND(STDDEV(sqft), 2) stddev_sqft
    FROM stores
    WHERE opened_date > '2024-04-04'
    GROUP BY DATE_TRUNC('MONTH', opened_date)
) sub
WHERE min_sqft < 13000
ORDER BY min_sqft;
