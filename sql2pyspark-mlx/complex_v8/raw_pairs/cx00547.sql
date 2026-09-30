WITH valid AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM policies
),
rollup_base AS (
    -- Summarise per group
    SELECT
        status,
        COUNT(DISTINCT status) AS distinct_status,
        MIN(end_date) AS first_end_date
    FROM valid
    GROUP BY status
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE distinct_status > (SELECT AVG(distinct_status) FROM rollup_base)
),
enriched AS (
    -- Label and score rows
    SELECT
        status,
        distinct_status,
        first_end_date,
        LOWER(TRIM(status)) AS status_clean,
        CASE WHEN distinct_status >= 8 THEN 'large' WHEN distinct_status >= 4 THEN 'medium' ELSE 'small' END AS distinct_status_tier,
        FLOOR(distinct_status / 2) * 2 AS distinct_status_bucket
    FROM screened
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(distinct_status_tier) OVER (PARTITION BY distinct_status, status)) AS distinct_distinct_status_tier_in_distinct_status,
        COUNT(*) OVER (PARTITION BY distinct_status, status) AS rows_in_distinct_status_status,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status ORDER BY distinct_status_bucket DESC), 3) AS distinct_status_bucket_percent_rank
    FROM enriched
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN status IN ('LAPSED', 'ACTIVE') THEN 'group_a' WHEN status = 'CANCELLED' THEN 'group_b' ELSE 'other' END AS status_group,
        ROUND(distinct_status_bucket / NULLIF(distinct_status, 0), 3) AS distinct_status_bucket_per_distinct_status
    FROM with_windows
    WHERE distinct_distinct_status_tier_in_distinct_status > 3
)
SELECT
    status,
    distinct_status,
    first_end_date,
    status_clean,
    distinct_status_tier,
    distinct_status_bucket,
    distinct_distinct_status_tier_in_distinct_status,
    rows_in_distinct_status_status,
    distinct_status_bucket_percent_rank,
    distinct_status_bucket_per_distinct_status,
    LEAST(distinct_status, distinct_status_bucket) AS least_distinct_status_distinct_status_bucket,
    LOWER(REGEXP_REPLACE(TRIM(status_clean), ' ', '_')) AS status_clean_slug
FROM scored
ORDER BY distinct_status_bucket DESC NULLS FIRST
