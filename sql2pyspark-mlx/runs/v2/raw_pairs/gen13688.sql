SELECT
    id,
    opened_date,
    sqft,
    region
FROM stores
WHERE opened_date >= (SELECT MAX(opened_date) FROM stores)
ORDER BY region DESC
