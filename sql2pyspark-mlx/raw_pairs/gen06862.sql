SELECT
    opened_date,
    store_name,
    region,
    ROUND(sqft, 1) AS sqft_rounded,
    SUBSTRING(region, 1, 4) AS region_prefix
FROM stores
WHERE city IN ('Tokyo', 'Bangkok');
