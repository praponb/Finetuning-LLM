WITH prepared_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        DATEDIFF(DATE '2025-06-30', reported_at) AS days_since_reported_at,
        approved_amount + claimed_amount AS approved_amount_plus_claimed_amount,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
    FROM claims
    WHERE status <> 'OPEN'
      AND claim_date < DATE '2024-12-22'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        approved_amount_plus_claimed_amount,
        claim_type_slug,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY policy_id, claim_type)) AS distinct_adjuster_id_in_policy_id_claim_type,
        LAST_VALUE(claimed_amount) OVER (ORDER BY approved_amount_plus_claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        FIRST_VALUE(days_since_reported_at) OVER (ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_days_since_reported_at
    FROM prepared_claims
),
with_activity AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.premium >= 2096.6
              AND p.start_date < DATE '2025-03-23'
        )
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claimed_amount,
        s.status,
        c.id AS claim_id,
        c.status AS claim_status,
        c.claim_type AS claim_claim_type
    FROM with_activity s
    JOIN claims c ON c.policy_id = s.policy_id AND c.status = 'IN_REVIEW'
),
with_parent_claim AS (
    -- Join claims
    SELECT
        s.id,
        s.policy_id,
        s.claimed_amount,
        s.status,
        s.claim_id,
        s.claim_claim_type,
        c.adjuster_id AS claim_adjuster_id,
        c.claimed_amount AS claim_claimed_amount,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM line_level s
    LEFT JOIN claims c ON c.id = s.claim_id
    JOIN adjusters a ON a.id = c.adjuster_id
),
joined_claim AS (
    -- Join claims
    SELECT
        s.id,
        s.policy_id,
        s.claimed_amount,
        s.status,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_claimed_amount,
        s.adjuster_name,
        s.adjuster_team,
        c.adjuster_id AS claim_adjuster_id_2,
        c.policy_id AS claim_policy_id
    FROM with_parent_claim s
    JOIN claims c ON c.id = s.claim_id
)
-- Final projection
SELECT
    id,
    policy_id,
    claimed_amount,
    claim_id,
    claim_adjuster_id,
    claim_claimed_amount,
    adjuster_name,
    claim_adjuster_id_2
FROM joined_claim
WHERE claim_adjuster_id IS NOT NULL
ORDER BY id DESC, claimed_amount
