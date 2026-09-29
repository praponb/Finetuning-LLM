SELECT
    id,
    city,
    sqft,
    ROUND(sqft * 0.9, 2) AS sqft_adjusted
FROM stores
WHERE region IS NULL AND store_name LIKE '%Riverside%' AND city IN ('Chiang Mai', 'Bangkok', 'London');
