WITH base AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref
    FROM claims
    WHERE policy_id IS NOT NULL
),
without_matches AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.start_date < DATE '2025-02-14'
              AND p.product IN ('LIFE', 'AUTO', 'HEALTH')
        )
),
with_details_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        a.team AS adjuster_team,
        p.status AS policy_status,
        p.premium AS policy_premium
    FROM without_matches s
    JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
    WHERE a.team NOT IN ('Motor', 'Travel')
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM with_details_adjuster
    WHERE approved_amount < (SELECT MAX(approved_amount) FROM with_details_adjuster)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    approved_amount,
    status,
    adjuster_team,
    policy_premium,
    CASE WHEN adjuster_team IN ('Motor', 'Property') THEN 'group_a' WHEN adjuster_team = 'Travel' THEN 'group_b' ELSE 'other' END AS adjuster_team_group,
    FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket
FROM screened
ORDER BY claim_date DESC NULLS FIRST, status
