SELECT *
FROM (
    SELECT
        DATE_TRUNC('MONTH', opened_date) AS opened_month,
        region,
        SUM(sqft) AS total_sqft,
        ROUND(STDDEV(sqft), 2) AS stddev_sqft
    FROM stores
    WHERE city IN ('New York', 'Chiang Mai')
    GROUP BY DATE_TRUNC('MONTH', opened_date), region
) AS agg
WHERE stddev_sqft > 69;
