WITH filtered AS (
    -- Keep valid rows and derive helper columns
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
        reported_at + INTERVAL 1 DAYS AS reported_at_plus_1d
    FROM claims
    WHERE claim_type IS NOT NULL
      AND YEAR(claim_date) = 2025
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.reported_at_plus_1d,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claimed_amount AS claim_claimed_amount,
        c.policy_id AS claim_policy_id
    FROM filtered s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        ROUND(AVG(claimed_amount), 2) AS avg_claim_claimed_amount,
        MAX(approved_amount) AS max_claim_approved_amount,
        MAX(claim_date) AS last_claim_date
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.avg_claim_claimed_amount,
        c.max_claim_approved_amount,
        c.last_claim_date
    FROM exploded_detail s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
)
SELECT
    id,
    adjuster_id,
    claim_date,
    claim_type,
    approved_amount,
    status,
    reported_at_plus_1d,
    claim_id,
    claim_reported_at,
    claim_claimed_amount,
    claim_policy_id,
    avg_claim_claimed_amount,
    last_claim_date,
    CASE WHEN approved_amount >= 20843.1 THEN 'HIGH' WHEN approved_amount >= 5336.7 THEN 'MEDIUM' ELSE 'LOW' END AS approved_amount_tier
FROM with_counts
ORDER BY id DESC NULLS FIRST, claim_id
LIMIT 10
