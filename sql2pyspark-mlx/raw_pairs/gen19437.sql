SELECT
    id,
    region,
    sqft,
    RANK() OVER (PARTITION BY region ORDER BY sqft DESC) AS rnk
FROM stores
WHERE region IN ('South', 'East')
ORDER BY sqft DESC
