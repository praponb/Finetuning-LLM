WITH scoped AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        CASE WHEN premium >= 3521.8 THEN 'large' WHEN premium >= 2289.0 THEN 'medium' ELSE 'small' END AS premium_tier
    FROM policies
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(status) OVER w_start_date_year_premium_tier) AS distinct_status_in_start_date_year_premium_tier,
        NTILE(4) OVER (PARTITION BY holder_id ORDER BY premium, id) AS premium_quartile,
        SUM(premium) OVER (PARTITION BY YEAR(start_date), premium_tier) AS start_date_year_premium_tier_sum_premium,
        MIN(premium) OVER (PARTITION BY YEAR(start_date), premium_tier) AS start_date_year_premium_tier_min_premium,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY status, DATE_TRUNC('MONTH', start_date) ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date
    FROM scoped
    WINDOW w_start_date_year_premium_tier AS (PARTITION BY YEAR(start_date), premium_tier)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id, premium_tier ORDER BY first_known_start_date, id), 3) AS first_known_start_date_percent_rank,
        COUNT(*) OVER w_holder_id AS rows_in_holder_id,
        SIZE(COLLECT_SET(premium_tier) OVER (PARTITION BY holder_id)) AS distinct_premium_tier_in_holder_id,
        RANK() OVER (PARTITION BY holder_id, premium_tier ORDER BY first_known_start_date) AS first_known_start_date_rank
    FROM metrics
    WINDOW w_holder_id AS (PARTITION BY holder_id)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('-', status, premium_tier) AS status_premium_tier_label,
        DATE_TRUNC('QUARTER', start_date) AS start_date_quarter
    FROM windowed
)
-- Final projection
SELECT
    id,
    holder_id,
    premium,
    start_date,
    status,
    premium_tier,
    distinct_status_in_start_date_year_premium_tier,
    start_date_year_premium_tier_sum_premium,
    start_date_year_premium_tier_min_premium,
    first_known_start_date,
    rows_in_holder_id,
    first_known_start_date_rank,
    status_premium_tier_label,
    start_date_quarter,
    LEAST(premium_quartile, premium) AS least_premium_quartile_premium
FROM enriched
ORDER BY least_premium_quartile_premium, first_known_start_date DESC
