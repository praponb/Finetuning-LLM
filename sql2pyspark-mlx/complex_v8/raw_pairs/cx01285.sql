WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref,
        reported_at + INTERVAL 7 DAYS AS reported_at_plus_7d
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-19 13:00:00'
      AND claim_date >= DATE '2024-07-20'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DATEDIFF(claim_date, LAG(claim_date) OVER (ORDER BY reported_at_plus_7d ASC, id)) AS days_since_prev_claim_date,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id,
        NTILE(10) OVER (ORDER BY reported_at_plus_7d ASC, id) AS reported_at_plus_7d_decile,
        LAG(reported_at_plus_7d, 2) OVER w_all_by_reported_at_plus_7d AS prev2_reported_at_plus_7d
    FROM recent
    WINDOW w_all_by_reported_at_plus_7d AS (ORDER BY reported_at_plus_7d ASC, id)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status_ref,
        reported_at_plus_7d,
        days_since_prev_claim_date,
        rows_in_policy_id,
        reported_at_plus_7d_decile,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_type ORDER BY reported_at_plus_7d_decile ASC), 3) AS reported_at_plus_7d_decile_cume_dist,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY policy_id, claim_type), 0), 4) AS claimed_amount_share_of_policy_id_claim_type,
        ROUND(AVG(days_since_prev_claim_date) OVER (PARTITION BY claim_type ORDER BY reported_at_plus_7d_decile ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS days_since_prev_claim_date_avg_2
    FROM metrics
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status_ref,
        days_since_prev_claim_date,
        rows_in_policy_id,
        reported_at_plus_7d_decile,
        claimed_amount_share_of_policy_id_claim_type,
        days_since_prev_claim_date_avg_2,
        SUBSTRING(TRIM(status_ref), 1, 2) AS status_ref_prefix,
        CASE WHEN rows_in_policy_id >= 15 THEN 'HIGH' WHEN rows_in_policy_id >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS rows_in_policy_id_tier
    FROM sequenced
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status_ref,
        reported_at_plus_7d_decile,
        claimed_amount_share_of_policy_id_claim_type,
        days_since_prev_claim_date_avg_2,
        rows_in_policy_id_tier,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY reported_at_plus_7d_decile, policy_id), 2) AS claimed_amount_vs_reported_at_plus_7d_decile_pol,
        RANK() OVER (PARTITION BY claim_type ORDER BY claimed_amount, id) AS claimed_amount_rank
    FROM scored
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount,
    status_ref,
    reported_at_plus_7d_decile,
    claimed_amount_share_of_policy_id_claim_type,
    days_since_prev_claim_date_avg_2,
    rows_in_policy_id_tier,
    claimed_amount_rank
FROM sequenced_2
WHERE claim_type IS NOT NULL
ORDER BY claimed_amount DESC
