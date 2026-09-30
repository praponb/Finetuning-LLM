WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount
    FROM claims
    WHERE claimed_amount > 7204.1
      AND reported_at >= TIMESTAMP '2025-01-31 11:00:00'
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.approved_amount < 24718.5
              AND c.policy_id IS NOT NULL
        )
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.status_slug,
        s.greatest_approved_amount_claimed_amount,
        c.id AS claim_id,
        c.status AS claim_status,
        c.claim_date
    FROM qualified s
    JOIN claims c ON c.policy_id = s.policy_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        adjuster_id,
        claim_type,
        status,
        greatest_approved_amount_claimed_amount,
        claim_id,
        claim_status,
        claim_date,
        CASE WHEN status IN ('APPROVED', 'OPEN') THEN 'group_a' WHEN status = 'IN_REVIEW' THEN 'group_b' ELSE 'other' END AS status_group,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        CONCAT_WS('|', claim_status, status) AS claim_status_status_label
    FROM with_children
    WHERE status_slug IS NOT NULL
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.adjuster_id,
        s.claim_type,
        s.status,
        s.greatest_approved_amount_claimed_amount,
        s.claim_id,
        s.claim_status,
        s.claim_date,
        s.claim_status_status_label,
        c.id AS claim_id_2,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount,
        c.reported_at AS claim_reported_at,
        c.policy_id AS claim_policy_id
    FROM enriched s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
)
SELECT
    id,
    adjuster_id,
    status,
    greatest_approved_amount_claimed_amount,
    claim_id,
    claim_date,
    claim_id_2,
    claim_approved_amount,
    claim_claimed_amount,
    claim_reported_at,
    claim_policy_id,
    DAYOFWEEK(claim_date) AS claim_date_dayofweek,
    COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
FROM exploded_detail_claims
WHERE claim_status_status_label IS NOT NULL
ORDER BY id
