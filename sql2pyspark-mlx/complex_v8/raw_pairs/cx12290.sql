WITH valid AS (
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
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_label,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM claims
),
with_details AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.claim_date_label,
        s.claim_type_or_unknown,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM valid s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT *
FROM (
    SELECT status, reported_at, claim_type_or_unknown, claimed_amount
    FROM with_details
) src
PIVOT (
    COUNT(claimed_amount)
    FOR claim_type_or_unknown IN ('flood', 'theft', 'delay', 'UNKNOWN')
)
ORDER BY status, reported_at
