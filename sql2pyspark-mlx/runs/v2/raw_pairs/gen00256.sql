SELECT
    id,
    city,
    sqft,
    region
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS rn
    FROM stores
    WHERE region IN ('North', 'Central', 'South')
) AS t
WHERE rn <= 5
ORDER BY region, city;
