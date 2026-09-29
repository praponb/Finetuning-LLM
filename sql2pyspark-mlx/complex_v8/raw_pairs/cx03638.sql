WITH filtered AS (
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
        claimed_amount - approved_amount AS claimed_amount_minus_approved_amount,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        TO_DATE(reported_at) AS reported_at_date
    FROM claims
    WHERE claim_type NOT IN ('delay', 'fire')
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.claimed_amount_minus_approved_amount,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_date AS claim_claim_date
    FROM filtered s
    JOIN claims c ON c.policy_id = s.policy_id
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM exploded_detail_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.status IS NOT NULL
              AND c.adjuster_id >= 2
        )
      AND s.status <> 'REJECTED'
),
enriched_claim AS (
    -- Attach claim attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.claimed_amount_minus_approved_amount,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_claim_date,
        c.approved_amount AS claim_approved_amount,
        a.team AS adjuster_team
    FROM with_activity s
    JOIN claims c ON c.id = s.claim_id
    JOIN adjusters a ON a.id = s.claim_adjuster_id
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.claimed_amount_minus_approved_amount,
        s.claim_id,
        s.claim_adjuster_id,
        s.adjuster_team,
        c.id AS claim_id_2,
        c.approved_amount AS claim_approved_amount_2,
        c.status AS claim_status,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_date
    FROM enriched_claim s
    LEFT JOIN claims c ON c.policy_id = s.policy_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, claim_id, metric, metric_value
FROM (
    SELECT id, claim_id, claim_approved_amount_2, claimed_amount_minus_approved_amount, claim_claimed_amount
    FROM line_level
) src
UNPIVOT (
    metric_value FOR metric IN (claim_approved_amount_2, claimed_amount_minus_approved_amount, claim_claimed_amount)
) unp
