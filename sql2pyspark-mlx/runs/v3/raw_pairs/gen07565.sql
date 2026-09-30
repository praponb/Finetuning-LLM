SELECT
    sqft,
    opened_date,
    region,
    LOWER(city) AS city_lower,
    LAST_DAY(opened_date) AS opened_date_month_end
FROM stores
WHERE sqft >= 14258;
