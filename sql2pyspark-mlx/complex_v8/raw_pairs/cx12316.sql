WITH base AS (
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
        CONCAT_WS(': ', status, claim_type) AS status_claim_type_label,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_label,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM claims
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        s.status_claim_type_label,
        s.claim_date_label,
        s.status_or_unknown,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_date AS claim_claim_date,
        c.claimed_amount AS claim_claimed_amount
    FROM base s
    JOIN claims c ON c.policy_id = s.policy_id AND c.adjuster_id IS NOT NULL
),
joined_claim AS (
    -- Attach claim attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status_claim_type_label,
        s.claim_date_label,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_claimed_amount,
        c.policy_id AS claim_policy_id
    FROM with_children_claims s
    LEFT JOIN claims c ON c.id = s.claim_id
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        approved_amount,
        claim_id,
        claim_adjuster_id,
        claim_policy_id,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        COALESCE(claim_claimed_amount, 0.0) AS claim_claimed_amount_filled,
        COALESCE(claim_claimed_amount, 0.0) AS claim_claimed_amount_filled_2
    FROM joined_claim
),
with_children_claims_2 AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_policy_id,
        s.claim_claimed_amount_filled,
        s.claim_claimed_amount_filled_2,
        c.id AS claim_id_2,
        c.claim_type AS claim_claim_type,
        c.reported_at AS claim_reported_at,
        c.adjuster_id AS claim_adjuster_id_2
    FROM derived s
    JOIN claims c ON c.policy_id = s.policy_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_policy_id,
        s.claim_claimed_amount_filled,
        s.claim_claimed_amount_filled_2,
        s.claim_id_2,
        s.claim_claim_type,
        s.claim_reported_at,
        s.claim_adjuster_id_2,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM with_children_claims_2 s
    JOIN adjusters a ON a.id = s.claim_adjuster_id_2
)
SELECT
    id,
    policy_id,
    claim_id,
    claim_adjuster_id,
    claim_policy_id,
    claim_claimed_amount_filled_2,
    claim_id_2,
    claim_claim_type,
    adjuster_team
FROM combined_details
WHERE claim_claim_type IN ('medical', 'delay')
ORDER BY claim_claim_type DESC NULLS FIRST
