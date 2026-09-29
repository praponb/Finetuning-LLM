WITH base_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at,
        SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix,
        DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label,
        LENGTH(TRIM(status)) AS status_len
    FROM claims
    WHERE approved_amount BETWEEN 2272.7 AND 32510.7
      AND adjuster_id <= 5
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_ADD(claim_date, 7) AS claim_date_plus_7d,
        SUBSTRING(TRIM(claim_date_label), 1, 4) AS claim_date_label_prefix
    FROM base_claims
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claim_type_prefix,
        s.claim_date_label_prefix,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claim_date AS claim_claim_date,
        c.claim_type AS claim_claim_type,
        c.status AS claim_status
    FROM tagged s
    JOIN claims c ON c.policy_id = s.policy_id
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        MAX(claimed_amount) AS max_claim_claimed_amount,
        MAX(reported_at) AS last_claim_reported_at
    FROM claims
    GROUP BY adjuster_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.max_claim_claimed_amount,
        c.last_claim_reported_at
    FROM detail_rows s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    claim_type_prefix,
    claim_date_label_prefix,
    claim_id,
    claim_reported_at,
    claim_claim_date,
    claim_claim_type,
    claim_status,
    max_claim_claimed_amount,
    last_claim_reported_at
FROM with_counts
ORDER BY claim_claim_date DESC
