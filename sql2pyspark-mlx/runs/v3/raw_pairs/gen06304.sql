SELECT
    id,
    region,
    sqft,
    ADD_MONTHS(opened_date, 12) AS opened_date_plus_months
FROM stores
WHERE city = 'New York'
