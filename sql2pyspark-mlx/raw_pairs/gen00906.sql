WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        CASE WHEN region IN ('West', 'North') THEN 'Tier 1' ELSE 'Other' END AS region_group,
        COALESCE(region, 'DIRECT') AS region_clean
    FROM stores
    WHERE region IN ('West', 'North')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        opened_date,
        sqft,
        region_clean,
        LEAD(sqft, 2) OVER (PARTITION BY city, region ORDER BY sqft ASC, id ASC) AS next_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY MONTH(opened_date), city), 4) AS sqft_share,
        NTILE(5) OVER (PARTITION BY city ORDER BY sqft DESC, id ASC) AS sqft_bucket
    FROM filtered
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        sqft_bucket,
        DENSE_RANK() OVER w_running AS sqft_dense_rank,
        AVG(sqft) OVER (PARTITION BY city) AS city_avg_sqft,
        SUM(sqft_bucket) OVER w_running_2 AS cumulative_sqft_bucket,
        AVG(next_sqft) OVER w_running_3 AS next_sqft_avg_2_prior
    FROM enriched
    WINDOW w_running AS (PARTITION BY MONTH(opened_date), city ORDER BY sqft ASC NULLS FIRST), w_running_2 AS (PARTITION BY city ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running_3 AS (ORDER BY sqft_bucket ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.status NOT IN ('PENDING', 'CANCELLED')
        )
),
not_flagged_stores AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.product_id IN (2, 6)
        )
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        city,
        CASE WHEN sqft_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END AS sqft_dense_rank_tier,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list,
        MIN(sqft_dense_rank) AS min_sqft_dense_rank,
        COUNT_IF(city = 'Berlin') AS berlin_events,
        SIZE(COLLECT_SET(city)) AS num_cities,
        COUNT_IF(city = 'Bangkok') AS bangkok_events
    FROM not_flagged_stores
    GROUP BY GROUPING SETS ((city, CASE WHEN sqft_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END), (city))
    HAVING COUNT_IF(city = 'Berlin') > 1
)
SELECT
    city,
    sqft_dense_rank_tier,
    min_sqft_dense_rank,
    num_cities,
    bangkok_events
FROM summary
ORDER BY bangkok_events DESC, num_cities DESC;
