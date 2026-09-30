SELECT
    id,
    opened_date,
    city,
    sqft
FROM stores
WHERE city = 'Chiang Mai' AND region IN ('South', 'North', 'West') AND opened_date BETWEEN '2024-09-27' AND '2025-01-01';
