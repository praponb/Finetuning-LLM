SELECT
    id,
    region,
    opened_date,
    CONCAT_WS('-', store_name, region) AS store_name_region_key
FROM stores
ORDER BY 3
