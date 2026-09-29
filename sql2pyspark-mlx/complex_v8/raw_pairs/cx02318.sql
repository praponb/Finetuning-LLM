WITH valid_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        DATEDIFF(DATE '2026-01-01', reported_at) AS days_since_reported_at
    FROM claims
    WHERE status NOT IN ('IN_REVIEW', 'APPROVED')
      AND policy_id >= 11
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        c.id AS claim_id,
        c.claim_type AS claim_claim_type,
        c.claimed_amount AS claim_claimed_amount,
        c.reported_at AS claim_reported_at,
        c.policy_id AS claim_policy_id
    FROM valid_claims s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
deduped AS (
    -- Keep the latest row per policy_id, claim_type
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        claim_id,
        claim_claim_type,
        claim_claimed_amount,
        claim_reported_at,
        claim_policy_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id, claim_type ORDER BY claim_reported_at DESC, id DESC, claim_id DESC) AS row_num
        FROM line_level_claims
    ) ranked
    WHERE row_num = 1
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
        s.claim_id,
        s.claim_claim_type,
        s.claim_claimed_amount,
        s.claim_policy_id,
        c.id AS claim_id_2,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at_2
    FROM deduped s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        TRUNC(claim_date, 'MM') AS claim_date_month_start,
        COALESCE(claim_claim_type, 'UNKNOWN') AS claim_claim_type_or_unknown
    FROM with_children_claims
    WHERE policy_id IS NOT NULL
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    claimed_amount,
    approved_amount,
    claim_id,
    claim_claim_type,
    claim_claimed_amount,
    claim_id_2,
    claim_status,
    claim_reported_at_2,
    claim_date_month_start,
    claim_claim_type_or_unknown,
    claim_reported_at_2 + INTERVAL 2 HOURS AS claim_reported_at_2_plus_2h,
    CASE WHEN approved_amount >= 14993.0 THEN 'A' WHEN approved_amount >= 7204.1 THEN 'B' ELSE 'C' END AS approved_amount_tier
FROM enriched
WHERE claim_claim_type NOT IN ('flood', 'collision')
ORDER BY claim_reported_at_2_plus_2h
