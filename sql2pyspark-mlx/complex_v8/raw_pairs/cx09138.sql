WITH cleaned_claims AS (
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
        ROUND(claimed_amount * 1.5, 2) AS claimed_amount_adj,
        LENGTH(TRIM(status)) AS status_len,
        TO_DATE(reported_at) AS reported_at_date
    FROM claims
    WHERE status <> 'OPEN'
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.status,
        s.claimed_amount_adj,
        s.status_len,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claim_date AS claim_claim_date
    FROM cleaned_claims s
    JOIN claims c ON c.adjuster_id = s.adjuster_id AND c.approved_amount BETWEEN 1273.7 AND 39988.0
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', claim_claim_date) AS days_since_claim_claim_date,
        status_len + claim_approved_amount AS status_len_plus_claim_approved_amount
    FROM detail_rows_claims
    WHERE policy_id IS NOT NULL
),
joined_claim AS (
    -- Attach claim attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount_adj,
        s.status_len,
        s.claim_id,
        s.claim_approved_amount,
        s.claim_claim_date,
        s.status_len_plus_claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount,
        p.status AS policy_status,
        p.end_date AS policy_end_date
    FROM labeled s
    JOIN claims c ON c.id = s.claim_id
    JOIN policies p ON p.id = s.policy_id
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM joined_claim
    WHERE claim_claimed_amount > (SELECT AVG(claim_claimed_amount) FROM joined_claim)
),
latest AS (
    -- Keep the latest row per adjuster_id, policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount_adj,
        status_len,
        claim_id,
        claim_approved_amount,
        claim_claim_date,
        status_len_plus_claim_approved_amount,
        claim_claimed_amount,
        policy_status,
        policy_end_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id, policy_id ORDER BY policy_end_date DESC, id DESC, claim_id DESC) AS row_num
        FROM without_matches
    ) ranked
    WHERE row_num = 1
),
with_parent_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount_adj,
        s.status_len,
        s.claim_id,
        s.claim_claim_date,
        s.status_len_plus_claim_approved_amount,
        s.claim_claimed_amount,
        s.policy_end_date,
        a.team AS adjuster_team,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM latest s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    claimed_amount_adj,
    status_len,
    claim_id,
    claim_claim_date,
    status_len_plus_claim_approved_amount,
    claim_claimed_amount,
    policy_end_date,
    adjuster_team,
    adjuster_name,
    adjuster_level
FROM with_parent_adjuster
ORDER BY claim_type
