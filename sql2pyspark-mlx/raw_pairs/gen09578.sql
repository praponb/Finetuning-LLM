SELECT
    id,
    sqft,
    region,
    DATE_ADD(opened_date, 48) AS opened_date_plus_48d
FROM stores
WHERE city IN ('London', 'New York', 'Bangkok')
