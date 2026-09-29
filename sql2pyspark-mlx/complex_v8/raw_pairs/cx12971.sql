WITH base_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at,
        TO_DATE(reported_at) AS reported_at_date,
        CASE WHEN status IN ('OPEN', 'APPROVED') THEN 'group_a' WHEN status = 'PAID' THEN 'group_b' ELSE 'other' END AS status_group,
        LOWER(TRIM(status)) AS status_clean
    FROM claims
    WHERE claim_date >= DATE '2024-07-01'
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at_date,
        status_group,
        status_clean,
        CASE WHEN status_group IN ('group_a', 'other') THEN 'group_a' WHEN status_group = 'group_b' THEN 'group_b' ELSE 'other' END AS status_group_group
    FROM base_claims
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(status_group, 'ALL') AS status_group,
    policy_id,
    COALESCE(status_group_group, 'ALL') AS status_group_group,
    COUNT(DISTINCT policy_id) AS distinct_policy_ids,
    ROUND(SUM(claimed_amount), 2) AS total_claimed_amount,
    DATEDIFF(MAX(reported_at_date), MIN(reported_at_date)) AS reported_at_date_span_days
FROM annotated
GROUP BY ROLLUP(status_group, policy_id, status_group_group)
ORDER BY status_group NULLS LAST, policy_id NULLS LAST, status_group_group NULLS LAST
