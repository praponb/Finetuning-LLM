WITH scoped AS (
    -- Scope the claims rows
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
        CASE WHEN claimed_amount >= 41760.8 THEN 'large' WHEN claimed_amount >= 11235.6 THEN 'medium' ELSE 'small' END AS claimed_amount_tier
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-03-02 11:00:00'
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        COUNT(*) FILTER (WHERE claim_type NOT IN ('medical', 'collision')) AS claim_claim_type_count,
        MAX(approved_amount) AS max_claim_approved_amount,
        MIN(claim_date) AS first_claim_date
    FROM claims
    GROUP BY adjuster_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count,
        c.max_claim_approved_amount,
        c.first_claim_date
    FROM scoped s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
without_matches AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM with_stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.product NOT IN ('HEALTH', 'LIFE')
              AND p.status NOT IN ('ACTIVE', 'CANCELLED')
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, approved_amount, claimed_amount
    FROM without_matches
) src
UNPIVOT (
    metric_value FOR measure IN (approved_amount AS approved_amount_v, claimed_amount AS claimed_amount_v)
) unp
ORDER BY id, measure
