WITH enriched AS (
    SELECT
        id,
        store_name,
        LEAD(sqft, 2) OVER (PARTITION BY city, region ORDER BY sqft, id) AS next_sqft,
        LAST_VALUE(region) IGNORE NULLS OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region,
        LAST_VALUE(region) IGNORE NULLS OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region_2,
        CUME_DIST() OVER (ORDER BY sqft NULLS FIRST) AS sqft_cume_dist
    FROM stores
)
SELECT
    id,
    store_name,
    next_sqft,
    last_known_region_2
FROM enriched;
