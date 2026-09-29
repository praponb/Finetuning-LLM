WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date,
        approved_amount - claimed_amount AS approved_amount_minus_claimed_amount
    FROM claims
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        days_since_claim_date,
        approved_amount_minus_claimed_amount,
        claimed_amount - days_since_claim_date AS claimed_amount_minus_days_since_claim_date
    FROM base
    WHERE adjuster_id <= 3
)
SELECT
    adjuster_id,
    policy_id,
    GROUPING_ID() AS gid,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT id) AS distinct_ids
FROM derived
GROUP BY GROUPING SETS ((adjuster_id, policy_id), (adjuster_id), (policy_id), ())
ORDER BY adjuster_id NULLS LAST, policy_id NULLS LAST
