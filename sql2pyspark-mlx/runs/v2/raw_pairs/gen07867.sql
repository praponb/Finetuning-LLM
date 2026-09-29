SELECT
    store_name,
    sqft,
    city,
    LOWER(city) AS city_lower,
    DATE_TRUNC('month', opened_date) AS opened_date_period
FROM stores
WHERE opened_date BETWEEN '2023-10-01' AND '2025-03-01'
