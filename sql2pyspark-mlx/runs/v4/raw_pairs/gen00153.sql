SELECT
    s.region,
    s.city,
    ss.product_id,
    CAST(ss.units AS DOUBLE) AS units_str
FROM stores s
LEFT JOIN store_sales ss ON s.id = ss.store_id
WHERE s.sqft <= 8752 OR s.opened_date > '2023-09-01'
ORDER BY units_str DESC
