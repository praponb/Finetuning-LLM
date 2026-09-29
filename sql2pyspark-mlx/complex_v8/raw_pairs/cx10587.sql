WITH cleaned_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        ABS(premium - 3521.8) AS premium_distance
    FROM policies
    WHERE start_date >= DATE '2024-05-07'
),
combined_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        p.segment AS holder_segment,
        p.holder_name
    FROM cleaned_policys s
    JOIN policyholders p ON p.id = s.holder_id AND p.segment IN ('VIP', 'RETAIL')
),
filtered AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM combined_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.premium < 3817.5
        )
      AND s.holder_segment <> 'VIP'
),
enriched AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.holder_segment,
        s.holder_name,
        p.region AS holder_region
    FROM filtered s
    JOIN policyholders p ON p.id = s.holder_id
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) AS claim_count,
        MAX(reported_at) AS last_claim_reported_at
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.last_claim_reported_at
    FROM enriched s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    end_date,
    holder_segment,
    holder_name,
    holder_region,
    claim_count,
    last_claim_reported_at,
    COALESCE(claim_count, 0) AS claim_count_filled,
    LOWER(REGEXP_REPLACE(TRIM(holder_name), ' ', '_')) AS holder_name_slug
FROM with_stats
ORDER BY claim_count_filled DESC NULLS FIRST
