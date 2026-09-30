WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        DATE_FORMAT(start_date, 'yyyy-MM-dd') AS start_date_label
    FROM policies
    WHERE status = 'ACTIVE'
      AND premium < 4417.0
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.status,
        s.start_date_label,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.reported_at AS claim_reported_at
    FROM cleaned_policys s
    LEFT JOIN claims c ON c.policy_id = s.id
),
derived AS (
    -- Add calculated columns
    SELECT *, ABS(claim_claimed_amount - 20436.5) AS claim_claimed_amount_distance
    FROM with_children_claims
    WHERE status <> 'CANCELLED'
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        status,
        start_date_label,
        claim_id,
        claim_claimed_amount,
        claim_reported_at,
        claim_claimed_amount_distance,
        LOWER(TRIM(start_date_label)) AS start_date_label_clean
    FROM derived
),
with_children_claims_2 AS (
    -- One row per claim
    SELECT
        s.id,
        s.status,
        s.start_date_label,
        s.claim_id,
        s.claim_claimed_amount,
        s.claim_claimed_amount_distance,
        c.id AS claim_id_2,
        c.claim_type,
        c.claimed_amount AS claim_claimed_amount_2,
        c.status AS claim_status,
        c.approved_amount AS claim_approved_amount
    FROM enriched s
    JOIN claims c ON c.policy_id = s.id AND c.claimed_amount <= 41878.5
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        ROUND(AVG(approved_amount), 2) AS avg_claim_approved_amount,
        MIN(reported_at) AS first_claim_reported_at
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.avg_claim_approved_amount,
        c.first_claim_reported_at
    FROM with_children_claims_2 s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
)
-- Final projection
SELECT
    id,
    status,
    claim_id,
    claim_claimed_amount_distance,
    claim_id_2,
    claim_type,
    claim_claimed_amount_2,
    claim_approved_amount,
    first_claim_reported_at
FROM with_stats
ORDER BY claim_type DESC NULLS FIRST
