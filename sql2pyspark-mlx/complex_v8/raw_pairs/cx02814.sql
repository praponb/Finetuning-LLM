WITH scoped AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE MONTH(reported_at) IN (1, 3)
      AND approved_amount >= 0.0
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.reported_at,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.status AS claim_status
    FROM scoped s
    JOIN claims c ON c.policy_id = s.policy_id
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN approved_amount >= 18478.3 THEN 'A' WHEN approved_amount >= 7204.1 THEN 'B' ELSE 'C' END AS approved_amount_tier,
        DATE_FORMAT(claim_reported_at, 'yyyy-MM-dd HH:00') AS claim_reported_at_label
    FROM with_children
)
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    approved_amount,
    reported_at,
    claim_id,
    claim_reported_at,
    approved_amount_tier,
    claim_reported_at_label,
    COALESCE(status, 'UNKNOWN') AS status_or_unknown
FROM annotated
WHERE claim_id >= 59
ORDER BY status_or_unknown, reported_at DESC
