WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(DATE '2025-06-30', end_date) AS days_since_end_date,
        LENGTH(TRIM(status)) AS status_len,
        ABS(premium - 3521.8) AS premium_distance
    FROM policies
    WHERE status NOT IN ('EXPIRED', 'CANCELLED')
      AND product = 'HEALTH'
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, MIN(start_date) AS first_policy_start_date
    FROM policies
    GROUP BY holder_id
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.first_policy_start_date
    FROM scoped_policys s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        days_since_end_date,
        status_len,
        premium_distance,
        GREATEST(days_since_end_date, status_len) AS greatest_days_since_end_date_status_len,
        CASE WHEN premium_distance >= 4000.0 THEN 'large' WHEN premium_distance >= 1250.0 THEN 'medium' ELSE 'small' END AS premium_distance_tier,
        DATEDIFF(start_date, first_policy_start_date) AS days_first_policy_start_date_to_start_date
    FROM profiled
    WHERE days_since_end_date > 280
),
rollup_base AS (
    -- Summarise per group
    SELECT
        product,
        MIN(days_since_end_date) AS min_days_since_end_date,
        MAX(days_since_end_date) AS max_days_since_end_date,
        COUNT(*) AS row_count
    FROM tagged
    GROUP BY product
    HAVING COUNT(*) >= 3
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM rollup_base
    WHERE max_days_since_end_date > (SELECT MIN(max_days_since_end_date) FROM rollup_base)
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(max_days_since_end_date / NULLIF(row_count, 0), 3) AS max_days_since_end_date_per_row_count,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown
    FROM with_activity
    WHERE max_days_since_end_date > 140
)
SELECT
    product,
    min_days_since_end_date,
    max_days_since_end_date,
    row_count,
    max_days_since_end_date_per_row_count,
    product_or_unknown
FROM enriched
