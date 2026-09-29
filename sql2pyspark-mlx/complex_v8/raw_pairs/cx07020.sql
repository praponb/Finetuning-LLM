WITH scoped_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at,
        ROUND(approved_amount * 1.5, 2) AS approved_amount_adj,
        CONCAT(claim_type, '#', CAST(policy_id AS STRING)) AS claim_type_ref,
        reported_at + INTERVAL 7 DAYS AS reported_at_plus_7d
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND claim_type IN ('fire', 'flood', 'medical')
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
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.last_claim_reported_at
    FROM scoped_claims s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
combined_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount_adj,
        s.claim_type_ref,
        s.claim_count,
        s.last_claim_reported_at,
        p.start_date AS policy_start_date,
        p.holder_id AS policy_holder_id,
        p.premium AS policy_premium
    FROM with_counts s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.premium BETWEEN 986.9 AND 3544.1
),
with_details_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount_adj,
        s.claim_type_ref,
        s.claim_count,
        s.policy_start_date,
        s.policy_holder_id,
        a.level AS adjuster_level,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM combined_details_policy s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.team <> 'Travel'
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.region AS holder_region,
        p.holder_name,
        p.joined_on AS holder_joined_on
    FROM with_details_adjuster s
    JOIN policyholders p ON p.id = s.policy_holder_id
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claim_type_ref,
    policy_start_date,
    holder_region,
    holder_joined_on,
    ABS(approved_amount_adj - 26250.0) AS approved_amount_adj_distance
FROM combined_details_holder
WHERE UPPER(TRIM(adjuster_name)) LIKE 'F%'
ORDER BY policy_start_date DESC NULLS FIRST, id
LIMIT 10
