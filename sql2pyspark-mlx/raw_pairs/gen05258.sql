SELECT
    id,
    region,
    CAST(sqft AS STRING) AS sqft_str,
    INITCAP(TRIM(region)) AS region_title
FROM stores
ORDER BY id DESC, region_title ASC;
