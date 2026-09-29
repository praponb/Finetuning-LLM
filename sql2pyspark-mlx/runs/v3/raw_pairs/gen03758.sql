SELECT
    sqft,
    region,
    CASE WHEN city IN ('Tokyo', 'Chiang Mai') THEN 'Priority' ELSE 'Other' END AS city_group,
    ROUND(sqft, 1) AS sqft_rounded
FROM stores
WHERE UPPER(TRIM(store_name)) = 'OLD TOWN';
