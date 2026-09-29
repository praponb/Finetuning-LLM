SELECT
    id,
    sqft,
    region,
    opened_date,
    LAST_DAY(opened_date) opened_date_month_end,
    DATE_TRUNC('YEAR', opened_date) opened_date_period
FROM stores
WHERE region != 'North' AND city = 'Chiang Mai' AND city NOT IN ('Chiang Mai', 'Berlin', 'Bangkok')
ORDER BY region;
