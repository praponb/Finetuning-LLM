WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(product), 1, 2) AS product_prefix,
        UPPER(TRIM(status)) AS status_clean
    FROM policies
    WHERE status IN ('ACTIVE', 'EXPIRED', 'LAPSED')
      AND premium >= 2013.8
),
latest AS (
    -- Keep the latest row per holder_id, status
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        product_prefix,
        status_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, status ORDER BY start_date DESC, id DESC) AS recency_rank
        FROM scoped
    ) ranked
    WHERE recency_rank = 1
),
holder_id_stats AS (
    -- Median premium per holder_id
    SELECT holder_id, PERCENTILE_APPROX(premium, 0.5) AS median_premium
    FROM latest
    GROUP BY holder_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_premium,
        ROUND(s.premium - g.median_premium, 2) AS premium_minus_median
    FROM latest s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.premium >= g.median_premium
),
with_parent AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.end_date,
        s.status,
        s.product_prefix,
        p.segment AS holder_segment,
        p.region AS holder_region,
        p.joined_on AS holder_joined_on
    FROM compared s
    JOIN policyholders p ON p.id = s.holder_id
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM with_parent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.approved_amount >= 0.0
        )
)
SELECT *
FROM (
    SELECT holder_id, holder_segment, status, premium
    FROM filtered
) src
PIVOT (
    SUM(premium)
    FOR status IN ('EXPIRED', 'CANCELLED')
)
ORDER BY holder_id, holder_segment
