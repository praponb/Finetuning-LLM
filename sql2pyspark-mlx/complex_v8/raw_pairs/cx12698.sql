WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref
    FROM policies
    WHERE status NOT IN ('LAPSED', 'EXPIRED')
      AND premium <= 3465.4
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MAX(p.start_date) FROM policies p WHERE p.holder_id = s.holder_id) AS last_policy_start_date,
        (SELECT MIN(p.start_date) FROM policies p WHERE p.holder_id = s.holder_id) AS first_policy_start_date
    FROM cleaned s
),
policy_stats_2 AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        MIN(premium) AS min_policy_premium,
        MIN(end_date) AS first_policy_end_date,
        COUNT(*) FILTER (WHERE status = 'LAPSED') AS policy_status_count
    FROM policies
    GROUP BY holder_id
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.min_policy_premium,
        p.first_policy_end_date,
        COALESCE(p.policy_status_count, 0) AS policy_status_count
    FROM summarized_rows s
    LEFT JOIN policy_stats_2 p ON p.holder_id = s.holder_id
)
-- Subtotals with CUBE
SELECT
    COALESCE(product, 'ALL') AS product,
    COALESCE(status, 'ALL') AS status,
    GROUPING_ID() AS grouping_level,
    MAX(last_policy_start_date) AS last_last_policy_start_date,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(premium, 0.5) AS median_premium
FROM with_stats
GROUP BY CUBE(product, status)
ORDER BY product NULLS LAST, status NULLS LAST
