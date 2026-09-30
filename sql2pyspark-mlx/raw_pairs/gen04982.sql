SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    COUNT_IF(city = 'Berlin') AS berlin_events,
    COUNT(DISTINCT region) AS distinct_regions,
    STDDEV_SAMP(sqft) AS sqft_stddev
FROM stores
GROUP BY DATE_TRUNC('WEEK', opened_date)
HAVING STDDEV_SAMP(sqft) > 0;
