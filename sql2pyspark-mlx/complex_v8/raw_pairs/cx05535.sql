WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        INITCAP(TRIM(status)) AS status_clean
    FROM claims
    WHERE (reported_at < TIMESTAMP '2025-02-24 07:00:00' OR claim_date BETWEEN DATE '2024-03-24' AND DATE '2025-12-03')
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        reported_at,
        status_clean,
        LEAD(claim_date, 3) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount, id) AS next3_claim_date,
        NTH_VALUE(reported_at, 2) OVER (ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at
    FROM valid
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN status IN ('OPEN', 'IN_REVIEW') THEN 'group_a' WHEN status = 'APPROVED' THEN 'group_b' ELSE 'other' END AS status_group,
        CASE WHEN claim_type IN ('fire', 'collision') THEN 'group_a' WHEN claim_type = 'delay' THEN 'group_b' ELSE 'other' END AS claim_type_group,
        DATEDIFF(claim_date, next3_claim_date) AS days_next3_claim_date_to_claim_date
    FROM with_trends
    WHERE claim_type = 'collision'
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    status,
    reported_at,
    status_clean,
    next3_claim_date,
    second_reported_at,
    status_group,
    days_next3_claim_date_to_claim_date,
    LOWER(TRIM(claim_type_group)) AS claim_type_group_clean
FROM annotated
ORDER BY status_group DESC, next3_claim_date DESC
