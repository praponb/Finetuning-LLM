WITH valid AS (
    SELECT
        *,
        CONCAT_WS('/', city, region) AS city_region_key,
        COALESCE(region, 'OTHER') AS region_filled
    FROM stores
    WHERE region IS NOT NULL
),
deduped AS (
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        region_filled
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date NULLS FIRST, id) = 1
),
valid_stores AS (
    SELECT
        *,
        REPLACE(store_name, ' ', '_') AS store_name_slug,
        LENGTH(region) AS region_length,
        COALESCE(region, 'DIRECT') AS region_clean
    FROM deduped
    WHERE region IS NOT NULL
      AND region IN ('East', 'Central')
),
metrics AS (
    SELECT
        id,
        store_name,
        region,
        opened_date,
        store_name_slug,
        region_length,
        region_clean,
        MIN(region_length) OVER (PARTITION BY region) AS region_min_region_length,
        NTILE(10) OVER (PARTITION BY region ORDER BY sqft, id) AS sqft_bucket,
        AVG(region_length) OVER w_ordered AS region_length_avg_6_prior,
        CUME_DIST() OVER (PARTITION BY region ORDER BY sqft NULLS LAST) AS sqft_cume_dist
    FROM valid_stores
    WINDOW w_ordered AS (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
)
SELECT
    CASE WHEN region_min_region_length >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END AS region_min_region_length_tier,
    region,
    APPROX_PERCENTILE(sqft_cume_dist, 0.9) AS sqft_cume_dist_p90,
    SIZE(COLLECT_SET(region)) AS num_regions,
    COUNT(CASE WHEN region = 'North' THEN 1 END) AS north_count,
    GROUPING(CASE WHEN region_min_region_length >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
FROM metrics
GROUP BY GROUPING SETS ((CASE WHEN region_min_region_length >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END, region), (CASE WHEN region_min_region_length >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY region_min_region_length_tier DESC NULLS FIRST
