WITH recent_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        CONCAT_WS(': ', claim_type, status) AS claim_type_status_label,
        CASE WHEN claim_type IN ('collision', 'theft') THEN 'group_a' WHEN claim_type = 'delay' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM claims
    WHERE claim_type IN ('theft', 'flood', 'collision')
),
rollup_base AS (
    -- Summarise per group
    SELECT
        adjuster_id,
        SUM(CASE WHEN status <> 'APPROVED' THEN 1 ELSE 0 END) AS n_status_approved,
        ROUND(100 * COUNT(CASE WHEN status IN ('REJECTED', 'APPROVED') THEN 1 END) / COUNT(*), 1) AS pct_status_in_rejected_approved,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids
    FROM recent_claims
    WHERE adjuster_id IS NOT NULL
    GROUP BY adjuster_id
    HAVING AVG(claimed_amount) > 11235.6
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(n_status_approved, distinct_policy_ids) AS greatest_n_status_approved_distinct_policy_ids
    FROM rollup_base
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.adjuster_id,
        s.pct_status_in_rejected_approved,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.reported_at AS claim_reported_at
    FROM derived s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
)
-- Final projection
SELECT
    adjuster_id,
    claim_reported_at,
    TO_DATE(claim_reported_at) AS claim_reported_at_date,
    HOUR(claim_reported_at) AS claim_reported_at_hour
FROM exploded_detail_claims
ORDER BY claim_reported_at, claim_reported_at_date
