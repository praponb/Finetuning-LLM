WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        COALESCE(premium, 0.0) AS premium_filled,
        CASE WHEN status IN ('EXPIRED', 'LAPSED') THEN 'group_a' WHEN status = 'ACTIVE' THEN 'group_b' ELSE 'other' END AS status_group
    FROM policies
    WHERE holder_id IS NOT NULL
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.status,
        s.status_or_unknown,
        s.premium_filled,
        s.status_group,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score
    FROM base s
    JOIN policyholders p ON p.id = s.holder_id
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM combined_details s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
        )
),
enriched_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.status,
        s.status_group,
        s.holder_joined_on,
        s.holder_risk_score,
        p.risk_score AS holder_risk_score_2,
        p.segment AS holder_segment,
        p.region AS holder_region
    FROM with_activity s
    JOIN policyholders p ON p.id = s.holder_id
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    status,
    status_group,
    holder_joined_on,
    holder_risk_score,
    holder_risk_score_2,
    holder_segment,
    holder_region,
    DATEDIFF(DATE '2026-01-01', holder_joined_on) AS days_since_holder_joined_on
FROM enriched_holder
WHERE holder_segment = 'RETAIL'
ORDER BY holder_segment
