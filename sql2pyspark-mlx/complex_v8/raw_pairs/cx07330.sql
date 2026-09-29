WITH scoped_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at,
        DATEDIFF(DATE '2026-01-01', reported_at) AS days_since_reported_at,
        CONCAT(claim_type, '#', CAST(adjuster_id AS STRING)) AS claim_type_ref,
        DATE_FORMAT(claim_date, 'MMM yyyy') AS claim_date_label
    FROM claims
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        reported_at,
        days_since_reported_at,
        claim_type_ref,
        ROW_NUMBER() OVER (PARTITION BY policy_id, adjuster_id ORDER BY days_since_reported_at DESC, id) AS rn,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_all_by_claim_date) AS days_since_prev_claim_date
    FROM scoped_claims
    WINDOW w_all_by_claim_date AS (ORDER BY claim_date ASC, id)
)
SELECT
    rn,
    policy_id,
    adjuster_id,
    GROUPING_ID() AS gid,
    MAX(claim_date) AS last_claim_date,
    MAX(days_since_reported_at) AS max_days_since_reported_at,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_trends
GROUP BY GROUPING SETS ((rn, policy_id, adjuster_id), (rn))
ORDER BY rn NULLS LAST, policy_id NULLS LAST, adjuster_id NULLS LAST
