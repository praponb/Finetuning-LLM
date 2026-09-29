WITH cleaned AS (
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        TRIM(store_name) AS clean_store_name
    FROM stores
    WHERE opened_date >= DATE '2023-03-26'
      AND region IS NOT NULL
      AND region IN ('South', 'East')
)
SELECT
    region,
    SUM(CASE WHEN city = 'Berlin' THEN sqft ELSE 0 END) AS berlin_sqft,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    PERCENTILE_APPROX(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles
FROM cleaned
GROUP BY region
ORDER BY region DESC NULLS LAST
