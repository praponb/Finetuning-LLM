WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN premium >= 3532.7 THEN 'large' WHEN premium >= 1541.8 THEN 'medium' ELSE 'small' END AS premium_tier
    FROM policies
    WHERE holder_id <= 15
      AND start_date < DATE '2025-05-30'
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        product,
        end_date,
        COALESCE(premium, 0.0) AS premium_filled,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
    FROM prepared
    WHERE premium >= 1042.8
),
holder_id_stats AS (
    -- Max premium_filled per holder_id
    SELECT holder_id, MAX(premium_filled) AS max_premium_filled
    FROM annotated
    GROUP BY holder_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_premium_filled,
        ROUND(s.premium_filled - g.max_premium_filled, 2) AS premium_filled_minus_max
    FROM annotated s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.premium_filled = g.max_premium_filled
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        COUNT(*) AS policy_count,
        COUNT(*) FILTER (WHERE product = 'HEALTH') AS policy_product_count,
        COUNT(*) AS policy_count_2
    FROM policies
    GROUP BY holder_id
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_count, 0) AS policy_count,
        COALESCE(p.policy_product_count, 0) AS policy_product_count,
        COALESCE(p.policy_count_2, 0) AS policy_count_2
    FROM outliers s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        product,
        end_date,
        premium_filled,
        days_start_date_to_end_date,
        status_ref,
        premium_filled_minus_max,
        policy_product_count,
        policy_count_2,
        COALESCE(policy_count_2, 0) AS policy_count_2_filled,
        ABS(premium_filled - 2360.0) AS premium_filled_distance,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown
    FROM profiled
    WHERE status_ref IS NOT NULL
),
without_matches AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status IN ('ACTIVE', 'LAPSED', 'EXPIRED')
        )
      AND s.premium_filled < 4280.0
)
SELECT
    id,
    holder_id,
    product,
    premium_filled,
    days_start_date_to_end_date,
    status_ref,
    premium_filled_minus_max,
    policy_product_count,
    policy_count_2,
    policy_count_2_filled,
    premium_filled_distance,
    product_or_unknown,
    LENGTH(TRIM(status_ref)) AS status_ref_len,
    LEAST(policy_count_2_filled, premium_filled_distance) AS least_policy_count_2_filled_premium_filled_dista
FROM without_matches
WHERE holder_id IS NOT NULL
ORDER BY status_ref DESC
