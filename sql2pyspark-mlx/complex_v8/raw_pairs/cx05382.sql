WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        reported_at
    FROM claims
    WHERE policy_id IS NOT NULL
),
rollup_base AS (
    -- Summarise per group
    SELECT
        adjuster_id,
        ROUND(AVG(approved_amount), 2) AS avg_approved_amount,
        COUNT(*) AS row_count,
        ROUND(SUM(approved_amount), 2) AS total_approved_amount,
        PERCENTILE_APPROX(approved_amount, 0.5) AS median_approved_amount,
        MAX(reported_at) AS last_reported_at,
        COUNT(*) FILTER (WHERE claim_type NOT IN ('delay', 'collision')) AS n_claim_type_not_in_delay_coll
    FROM recent_claims
    WHERE approved_amount <= 28426.9
    GROUP BY 1
    HAVING COUNT(*) >= 2
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(total_approved_amount - 105000.0) AS total_approved_amount_distance,
        ABS(total_approved_amount - 120000.0) AS total_approved_amount_distance_2
    FROM rollup_base
)
SELECT adjuster_id, metric, value
FROM (
    SELECT adjuster_id, total_approved_amount, total_approved_amount_distance, total_approved_amount_distance_2, avg_approved_amount
    FROM annotated
) src
UNPIVOT (
    value FOR metric IN (total_approved_amount, total_approved_amount_distance, total_approved_amount_distance_2, avg_approved_amount)
) unp
ORDER BY adjuster_id, metric
