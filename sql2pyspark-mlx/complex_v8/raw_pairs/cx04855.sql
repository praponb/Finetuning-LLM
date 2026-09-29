WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        reported_at,
        CONCAT(claim_type, '#', CAST(policy_id AS STRING)) AS claim_type_ref,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
    FROM claims
    WHERE status = 'IN_REVIEW'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_id,
        claim_type,
        approved_amount,
        claim_type_ref,
        claimed_amount_bucket,
        ROW_NUMBER() OVER w_all_by_reported_at AS seq_no,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (ORDER BY claimed_amount_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at,
        NTH_VALUE(claimed_amount_bucket, 2) OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount_bucket,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY policy_id)) AS distinct_adjuster_id_in_policy_id,
        ROUND(AVG(claimed_amount_bucket) OVER (ORDER BY claimed_amount_bucket DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_bucket_avg_4,
        MAX(approved_amount) OVER (PARTITION BY policy_id) AS policy_id_max_approved_amount,
        claimed_amount_bucket - LAG(claimed_amount_bucket) OVER w_all_by_claimed_amount_bucket_3 AS claimed_amount_bucket_change,
        ROUND(claimed_amount_bucket - AVG(claimed_amount_bucket) OVER (PARTITION BY policy_id), 2) AS claimed_amount_bucket_vs_policy_id_avg,
        MAX(claimed_amount_bucket) OVER (PARTITION BY adjuster_id ORDER BY approved_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claimed_amount_bucket_so_far,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY policy_id), 2) AS approved_amount_vs_policy_id_avg,
        DENSE_RANK() OVER w_all_by_reported_at_ties AS reported_at_dense_rank,
        LAG(reported_at, 2) OVER (ORDER BY claimed_amount_bucket DESC, id) AS prev2_reported_at
    FROM recent_claims
    WINDOW w_all_by_reported_at AS (ORDER BY reported_at, id), w_all_by_claimed_amount_bucket_3 AS (ORDER BY claimed_amount_bucket DESC, id), w_all_by_reported_at_ties AS (ORDER BY reported_at)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        adjuster_id,
        claim_type,
        approved_amount,
        claim_type_ref,
        claimed_amount_bucket,
        last_known_reported_at,
        second_claimed_amount_bucket,
        distinct_adjuster_id_in_policy_id,
        claimed_amount_bucket_avg_4,
        policy_id_max_approved_amount,
        claimed_amount_bucket_change,
        claimed_amount_bucket_vs_policy_id_avg,
        max_claimed_amount_bucket_so_far,
        approved_amount_vs_policy_id_avg,
        reported_at_dense_rank,
        prev2_reported_at
    FROM with_windows
    WHERE seq_no = 1
      AND approved_amount < 15857.9
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(last_known_reported_at) - UNIX_TIMESTAMP(LAG(last_known_reported_at) OVER (PARTITION BY reported_at_dense_rank, claim_type ORDER BY approved_amount, id))) / 60, 1) AS minutes_since_prev_last_known_reported_at,
        NTH_VALUE(second_claimed_amount_bucket, 2) OVER (PARTITION BY reported_at_dense_rank, claim_type ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_claimed_amount_bucket,
        ROUND(AVG(reported_at_dense_rank) OVER (ORDER BY second_claimed_amount_bucket, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS reported_at_dense_rank_avg_6_prior,
        LAG(approved_amount) OVER (ORDER BY second_claimed_amount_bucket, id) AS prev_approved_amount,
        ROUND(PERCENT_RANK() OVER (ORDER BY last_known_reported_at ASC), 3) AS last_known_reported_at_percent_rank
    FROM leaders
),
stats AS (
    -- Summarise per group
    SELECT
        YEAR(last_known_reported_at) AS last_known_reported_at_year,
        claim_type,
        COUNT(*) AS cnt,
        MIN(claimed_amount_bucket_vs_policy_id_avg) AS min_claimed_amount_bucket_vs_policy_id_avg,
        ROUND(SUM(minutes_since_prev_last_known_reported_at) / NULLIF(SUM(max_claimed_amount_bucket_so_far), 0), 3) AS minutes_since_prev_last_known_reported_at_to_max,
        ROUND(AVG(prev_approved_amount), 2) AS avg_prev_approved_amount,
        SUM(claimed_amount_bucket_vs_policy_id_avg) FILTER (WHERE claim_type IN ('fire', 'collision')) AS claimed_amount_bucket_vs_policy_id_avg_claim_typ
    FROM metrics
    GROUP BY 1, 2
)
SELECT
    last_known_reported_at_year,
    claim_type,
    cnt,
    min_claimed_amount_bucket_vs_policy_id_avg,
    minutes_since_prev_last_known_reported_at_to_max,
    avg_prev_approved_amount,
    claimed_amount_bucket_vs_policy_id_avg_claim_typ
FROM stats
WHERE claim_type IN ('fire', 'collision', 'medical')
ORDER BY minutes_since_prev_last_known_reported_at_to_max, last_known_reported_at_year, claim_type
LIMIT 5
