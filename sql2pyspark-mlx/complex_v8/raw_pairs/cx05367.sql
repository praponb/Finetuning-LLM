WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_TRUNC('QUARTER', start_date) AS start_date_quarter,
        CASE WHEN premium >= 3465.4 THEN 'large' WHEN premium >= 2289.0 THEN 'medium' ELSE 'small' END AS premium_tier,
        DATE_TRUNC('YEAR', end_date) AS end_date_year
    FROM policies
    WHERE product <> 'HOME'
),
qualified AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.premium > 2033.8
              AND p.start_date BETWEEN DATE '2024-10-22' AND DATE '2025-03-24'
        )
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.holder_name,
        p.segment AS holder_segment,
        p.joined_on AS holder_joined_on
    FROM qualified s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.joined_on < DATE '2024-12-21'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        product,
        premium,
        start_date,
        end_date,
        status,
        start_date_quarter,
        premium_tier,
        holder_name,
        holder_joined_on,
        MIN(premium) OVER (ORDER BY holder_joined_on NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_so_far,
        SIZE(COLLECT_SET(premium_tier) OVER (PARTITION BY status, TO_DATE(start_date_quarter))) AS distinct_premium_tier_in_status_start_date_quart,
        ROUND(PERCENT_RANK() OVER (ORDER BY end_date), 3) AS end_date_percent_rank,
        LAST_VALUE(holder_joined_on) IGNORE NULLS OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_holder_joined_on
    FROM enriched
),
latest AS (
    -- Keep the latest row per status
    SELECT
        id,
        product,
        premium,
        start_date,
        end_date,
        status,
        start_date_quarter,
        premium_tier,
        holder_name,
        holder_joined_on,
        min_premium_so_far,
        distinct_premium_tier_in_status_start_date_quart,
        end_date_percent_rank,
        last_known_holder_joined_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date DESC, id DESC) AS recency_rank
        FROM metrics
    ) ranked
    WHERE recency_rank = 1
),
rollup_base AS (
    -- Summarise per group
    SELECT
        YEAR(holder_joined_on) AS holder_joined_on_year,
        distinct_premium_tier_in_status_start_date_quart,
        ROUND(100 * COUNT(CASE WHEN holder_name IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_holder_name_is_not_null,
        PERCENTILE_APPROX(premium, 0.5) AS median_premium,
        PERCENTILE_APPROX(min_premium_so_far, 0.5) AS median_min_premium_so_far,
        MIN(holder_joined_on) AS first_holder_joined_on,
        COUNT(*) AS row_count,
        COUNT(DISTINCT premium_tier) AS distinct_premium_tiers
    FROM latest
    GROUP BY YEAR(holder_joined_on), distinct_premium_tier_in_status_start_date_quart
),
tagged AS (
    -- Add calculated columns
    SELECT
        holder_joined_on_year,
        distinct_premium_tier_in_status_start_date_quart,
        pct_holder_name_is_not_null,
        median_premium,
        row_count,
        distinct_premium_tiers,
        ABS(distinct_premium_tiers - 6) AS distinct_premium_tiers_distance
    FROM rollup_base
    WHERE median_premium > 2120.0
)
SELECT
    holder_joined_on_year,
    pct_holder_name_is_not_null,
    median_premium,
    row_count,
    distinct_premium_tiers,
    distinct_premium_tiers_distance,
    CASE WHEN row_count >= 8 THEN 'HIGH' WHEN row_count >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS row_count_tier
FROM tagged
ORDER BY distinct_premium_tiers_distance
