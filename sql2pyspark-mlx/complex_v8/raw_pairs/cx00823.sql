WITH prepared_claims AS (
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
        approved_amount - claimed_amount AS approved_amount_minus_claimed_amount,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        DATE_TRUNC('YEAR', reported_at) AS reported_at_year
    FROM claims
    WHERE claimed_amount > 18364.1
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claim_type_len,
        s.reported_at_year,
        c.id AS claim_id,
        c.claim_type AS claim_claim_type,
        c.status AS claim_status,
        c.policy_id AS claim_policy_id,
        c.reported_at AS claim_reported_at
    FROM prepared_claims s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
with_details_claim AS (
    -- Attach claim attributes
    SELECT
        s.*,
        c.adjuster_id AS claim_adjuster_id,
        c.reported_at AS claim_reported_at_2,
        c.approved_amount AS claim_approved_amount
    FROM exploded_detail_claims s
    JOIN claims c ON c.id = s.claim_id
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        COUNT(*) AS claim_count,
        ROUND(AVG(claimed_amount), 2) AS avg_claim_claimed_amount,
        SUM(approved_amount) AS total_claim_approved_amount
    FROM claims
    GROUP BY adjuster_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.avg_claim_claimed_amount,
        c.total_claim_approved_amount
    FROM with_details_claim s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
)
SELECT
    id,
    adjuster_id,
    claim_date,
    claim_type,
    reported_at_year,
    claim_id,
    claim_claim_type,
    claim_status,
    claim_reported_at,
    claim_approved_amount,
    total_claim_approved_amount
FROM with_counts
WHERE claim_type <> 'flood'
ORDER BY claim_reported_at, id, claim_id
LIMIT 5
