WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status
    FROM policies
    WHERE holder_id IS NOT NULL
      AND status = 'LAPSED'
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.end_date,
        s.status,
        p.segment AS holder_segment,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score
    FROM scoped_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
enriched_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.status,
        s.holder_segment,
        s.holder_joined_on,
        s.holder_risk_score,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score_2
    FROM enriched s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.risk_score > 0.3
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(holder_region), ' ', '_')) AS holder_region_slug
    FROM enriched_holder
    WHERE holder_joined_on BETWEEN DATE '2024-03-22' AND DATE '2025-05-01'
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        COUNT(*) FILTER (WHERE product NOT IN ('HEALTH', 'TRAVEL')) AS policy_product_count,
        MIN(end_date) AS first_policy_end_date,
        COUNT(CASE WHEN product = 'LIFE' THEN 1 END) AS policy_product_count_2
    FROM policies
    GROUP BY holder_id
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_product_count, 0) AS policy_product_count,
        p.first_policy_end_date,
        COALESCE(p.policy_product_count_2, 0) AS policy_product_count_2
    FROM labeled s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
enriched_holder_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.holder_segment,
        s.holder_risk_score,
        s.holder_region,
        s.holder_region_slug,
        s.policy_product_count,
        s.first_policy_end_date,
        p.joined_on AS holder_joined_on_2
    FROM with_counts s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.joined_on >= DATE '2024-07-18'
)
-- Final projection
SELECT
    id,
    holder_segment,
    holder_risk_score,
    holder_region_slug,
    policy_product_count,
    first_policy_end_date,
    holder_joined_on_2
FROM enriched_holder_2
WHERE premium >= 2096.6
ORDER BY policy_product_count DESC
