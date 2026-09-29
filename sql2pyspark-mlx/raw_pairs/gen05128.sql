WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(store_name) AS clean_store_name,
        COALESCE(region, 'OTHER') AS region_clean
    FROM stores
    WHERE city = 'Singapore'
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date >= s.opened_date
        )
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY sqft DESC, id) AS latest_rank
        FROM kept
    ) ranked
    WHERE latest_rank = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY latest_rank DESC, id) AS dedup_rank
        FROM deduped
    ) x
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        region,
        latest_rank,
        NTILE(3) OVER (PARTITION BY city ORDER BY sqft DESC, id) AS sqft_bucket,
        LAG(sqft, 2) OVER (PARTITION BY region ORDER BY dedup_rank, id) AS prev_sqft
    FROM latest
    WHERE region = 'South'
)
-- Aggregate with conditional metrics
SELECT
    region,
    SIZE(COLLECT_SET(region)) AS num_regions,
    COUNT(*) FILTER (WHERE region = 'North') AS north_rows,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    AVG(prev_sqft) AS avg_prev_sqft,
    COUNT_IF(region = 'East') AS east_events
FROM with_windows
GROUP BY region
ORDER BY num_regions
