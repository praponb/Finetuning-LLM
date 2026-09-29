WITH filtered AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment
    FROM policyholders
    WHERE (UPPER(TRIM(holder_name)) LIKE 'E%' OR risk_score >= 0.3)
),
with_children AS (
    -- One row per policy
    SELECT
        s.id,
        s.region,
        s.risk_score,
        s.joined_on,
        p.id AS policy_id,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date,
        p.status AS policy_status
    FROM filtered s
    JOIN policies p ON p.holder_id = s.id AND p.premium <= 4670.9
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.region,
        s.risk_score,
        s.joined_on,
        s.policy_id,
        s.policy_premium,
        s.policy_end_date,
        s.policy_status,
        p.end_date AS policy_end_date_2,
        p.start_date AS policy_start_date,
        p.holder_id AS policy_holder_id
    FROM with_children s
    JOIN policies p ON p.id = s.policy_id
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        MIN(end_date) AS first_policy_end_date,
        COUNT(*) AS policy_count
    FROM policies
    GROUP BY holder_id
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.first_policy_end_date,
        COALESCE(p.policy_count, 0) AS policy_count
    FROM with_parent s
    LEFT JOIN policy_stats p ON p.holder_id = s.id
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, ROUND(AVG(claimed_amount), 2) AS avg_claim_claimed_amount
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.avg_claim_claimed_amount
    FROM with_stats s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
)
-- Final projection
SELECT
    id,
    region,
    risk_score,
    joined_on,
    policy_id,
    policy_premium,
    policy_end_date,
    policy_end_date_2,
    policy_holder_id,
    policy_count,
    avg_claim_claimed_amount,
    DATE_ADD(policy_end_date_2, 7) AS policy_end_date_2_plus_7d,
    LOWER(REGEXP_REPLACE(TRIM(region), ' ', '_')) AS region_slug
FROM summarized_rows
ORDER BY risk_score DESC, id, policy_id
LIMIT 10
