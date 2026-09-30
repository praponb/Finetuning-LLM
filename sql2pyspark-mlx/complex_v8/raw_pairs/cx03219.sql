WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        reported_at,
        ABS(approved_amount - 4554.8) AS approved_amount_distance,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM claims
    WHERE claim_type IN ('fire', 'collision')
      AND HOUR(reported_at) BETWEEN 7 AND 10
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (ORDER BY claim_date, id))) / 60, 1) AS minutes_since_prev_reported_at,
        NTILE(3) OVER (PARTITION BY status ORDER BY claim_date DESC, id) AS claim_date_tercile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status, adjuster_id ORDER BY claim_date ASC, id), 3) AS claim_date_percent_rank,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY claim_type)) AS distinct_policy_id_in_claim_type,
        NTILE(3) OVER (PARTITION BY claim_type_or_unknown, policy_id ORDER BY claim_date ASC, id) AS claim_date_tercile_2,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY claim_date DESC, id) AS rn,
        ROUND(approved_amount_distance - AVG(approved_amount_distance) OVER w_claim_type, 2) AS approved_amount_distance_vs_claim_type_avg,
        SIZE(COLLECT_SET(claim_type_or_unknown) OVER w_adjuster_id) AS distinct_claim_type_or_unknown_in_adjuster_id,
        ROUND(AVG(approved_amount_distance) OVER (PARTITION BY claim_type_or_unknown, policy_id ORDER BY claim_date ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_distance_avg_2_prior,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY adjuster_id)) AS distinct_status_in_adjuster_id,
        ROUND(CUME_DIST() OVER (PARTITION BY status, adjuster_id ORDER BY claim_date ASC), 3) AS claim_date_cume_dist,
        ROUND(AVG(approved_amount_distance) OVER (PARTITION BY adjuster_id), 2) AS adjuster_id_avg_approved_amount_distance,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY claim_type)) AS distinct_adjuster_id_in_claim_type,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY claim_type_or_unknown, policy_id ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        MAX(approved_amount_distance) OVER w_adjuster_id AS adjuster_id_max_approved_amount_distance
    FROM prepared
    WINDOW w_claim_type AS (PARTITION BY claim_type), w_adjuster_id AS (PARTITION BY adjuster_id)
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    status,
    reported_at,
    claim_type_or_unknown,
    minutes_since_prev_reported_at,
    distinct_policy_id_in_claim_type,
    claim_date_tercile_2,
    rn,
    approved_amount_distance_vs_claim_type_avg,
    distinct_claim_type_or_unknown_in_adjuster_id,
    approved_amount_distance_avg_2_prior,
    distinct_status_in_adjuster_id,
    claim_date_cume_dist,
    adjuster_id_avg_approved_amount_distance,
    distinct_adjuster_id_in_claim_type,
    second_reported_at
FROM sequenced
ORDER BY distinct_status_in_adjuster_id DESC, policy_id, id
LIMIT 5
