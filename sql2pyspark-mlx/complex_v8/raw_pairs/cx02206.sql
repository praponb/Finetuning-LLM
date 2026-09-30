WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        reported_at,
        LENGTH(TRIM(claim_type)) AS claim_type_len
    FROM claims
    WHERE policy_id IS NOT NULL
      AND claimed_amount >= 18778.6
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type_len, policy_id ORDER BY claim_type_len DESC, id), 3) AS claim_type_len_percent_rank,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY claim_type_len)) AS distinct_policy_id_in_claim_type_len,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (PARTITION BY policy_id, claim_type ORDER BY claim_type_len, id))) / 60, 1) AS minutes_since_prev_reported_at,
        DENSE_RANK() OVER (PARTITION BY claim_type_len, policy_id ORDER BY claim_type_len DESC) AS claim_type_len_dense_rank,
        MAX(claim_type_len) OVER (PARTITION BY claim_type_len, adjuster_id ORDER BY claim_type_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claim_type_len_so_far,
        ROUND(AVG(claim_type_len) OVER w_claim_date_month_claim_type, 2) AS claim_date_month_claim_type_avg_claim_type_len,
        ROUND(PERCENT_RANK() OVER w_all_by_claim_date, 3) AS claim_date_percent_rank,
        RANK() OVER (PARTITION BY policy_id, claim_type ORDER BY claim_type_len) AS claim_type_len_rank,
        SUM(claim_type_len) OVER (PARTITION BY claim_type_len) AS claim_type_len_sum_claim_type_len,
        ROUND(CUME_DIST() OVER (ORDER BY claim_date DESC), 3) AS claim_date_cume_dist
    FROM scoped
    WINDOW w_claim_date_month_claim_type AS (PARTITION BY DATE_TRUNC('MONTH', claim_date), claim_type), w_all_by_claim_date AS (ORDER BY claim_date DESC, id)
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_date,
    claim_type,
    reported_at,
    claim_type_len,
    claim_type_len_percent_rank,
    minutes_since_prev_reported_at,
    max_claim_type_len_so_far,
    claim_date_month_claim_type_avg_claim_type_len,
    claim_date_percent_rank,
    claim_type_len_rank,
    claim_date_cume_dist,
    ROUND(claim_date_percent_rank * 0.9, 2) AS claim_date_percent_rank_adj
FROM ranked
ORDER BY max_claim_type_len_so_far
