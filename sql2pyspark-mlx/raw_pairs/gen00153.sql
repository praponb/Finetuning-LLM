WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        FLOOR(sqft) AS sqft_whole,
        COALESCE(region, 'DIRECT') AS resolved_region
    FROM stores
    WHERE opened_date > DATE '2023-07-31'
      AND region IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        region,
        sqft_whole,
        resolved_region,
        NTILE(3) OVER (ORDER BY sqft_whole DESC, id ASC) AS sqft_whole_bucket,
        SUM(sqft_whole) OVER (ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft_whole
    FROM valid
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY region ORDER BY running_sqft_whole DESC NULLS FIRST) AS dedup_rank
        FROM enriched
    ) t
    WHERE dedup_rank = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        region,
        sqft_whole,
        resolved_region,
        running_sqft_whole,
        dedup_rank,
        COALESCE(region, 'unassigned') AS region_filled,
        COALESCE(region, 'DIRECT') AS resolved_region_2
    FROM ranked
    WHERE region IS NOT NULL
),
enriched_stores AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY region ORDER BY sqft_whole DESC NULLS FIRST) AS sqft_whole_rank,
        CUME_DIST() OVER (PARTITION BY region ORDER BY running_sqft_whole DESC NULLS FIRST) AS running_sqft_whole_cume_dist,
        FIRST_VALUE(running_sqft_whole, TRUE) OVER (ORDER BY dedup_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_sqft_whole
    FROM base
    WHERE dedup_rank <= 1
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        region,
        ROUND(SUM(running_sqft_whole_cume_dist) / NULLIF(COUNT(CASE WHEN region = 'West' THEN 1 END), 0), 2) AS running_sqft_whole_cume_dist_per_west,
        AVG(running_sqft_whole_cume_dist) FILTER (WHERE region = 'Central') AS central_avg_running_sqft_whole_cume_dist,
        SIZE(COLLECT_SET(region)) AS num_regions,
        APPROX_PERCENTILE(sqft_whole_rank, 0.5) AS sqft_whole_rank_p50,
        GROUPING_ID() AS gid
    FROM enriched_stores
    GROUP BY CUBE (region)
)
SELECT
    running_sqft_whole_cume_dist_per_west,
    central_avg_running_sqft_whole_cume_dist,
    sqft_whole_rank_p50,
    gid
FROM daily
ORDER BY running_sqft_whole_cume_dist_per_west ASC
