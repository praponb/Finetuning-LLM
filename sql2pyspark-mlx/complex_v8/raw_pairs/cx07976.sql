WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at,
        DAYOFWEEK(claim_date) AS claim_date_dayofweek
    FROM claims
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_id,
        claim_date,
        claimed_amount,
        claim_date_dayofweek,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (PARTITION BY policy_id, adjuster_id ORDER BY claim_date_dayofweek, id))) / 60, 1) AS minutes_since_prev_reported_at,
        ROUND(CUME_DIST() OVER w_all_by_reported_at_ties, 3) AS reported_at_cume_dist,
        ROUND(PERCENT_RANK() OVER w_policy_id_adjuster_id_by_cla, 3) AS claim_date_dayofweek_percent_rank,
        ROUND(CUME_DIST() OVER (ORDER BY claim_date NULLS LAST), 3) AS claim_date_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY claim_date NULLS LAST, id), 3) AS claim_date_percent_rank,
        COUNT(*) OVER (PARTITION BY claim_date_dayofweek, policy_id) AS rows_in_claim_date_dayofweek_policy_id,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY claim_type)) AS distinct_policy_id_in_claim_type,
        SUM(claim_date_dayofweek) OVER (PARTITION BY claim_date_dayofweek, policy_id) AS claim_date_dayofweek_policy_id_sum_claim_date_da,
        LAST_VALUE(claimed_amount) OVER (ORDER BY claim_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        MIN(claim_date_dayofweek) OVER w_claim_date_dayofweek_policy AS claim_date_dayofweek_policy_id_min_claim_date_da
    FROM valid
    WINDOW w_all_by_reported_at_ties AS (ORDER BY reported_at), w_policy_id_adjuster_id_by_cla AS (PARTITION BY policy_id, adjuster_id ORDER BY claim_date_dayofweek, id), w_claim_date_dayofweek_policy AS (PARTITION BY claim_date_dayofweek, policy_id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        adjuster_id,
        claim_date,
        reported_at_cume_dist,
        claim_date_dayofweek_percent_rank,
        claim_date_cume_dist,
        claim_date_percent_rank,
        claim_date_dayofweek_policy_id_sum_claim_date_da,
        claim_date_dayofweek_policy_id_min_claim_date_da,
        DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date,
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month
    FROM metrics
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(claim_date_cume_dist) OVER (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', claim_date_month) ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_claim_date_cume_dist_so_far,
        DENSE_RANK() OVER (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', claim_date_month) ORDER BY claim_date DESC, id) AS claim_date_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY YEAR(claim_date) ORDER BY claim_date_month ASC, id) AS seq_no,
        LEAD(days_since_claim_date, 2) OVER (ORDER BY days_since_claim_date, id) AS next2_days_since_claim_date,
        ROUND(claim_date_percent_rank - AVG(claim_date_percent_rank) OVER (PARTITION BY adjuster_id), 2) AS claim_date_percent_rank_vs_adjuster_id_avg,
        NTH_VALUE(days_since_claim_date, 2) OVER (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', claim_date_month) ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_claim_date,
        LAG(days_since_claim_date) OVER (PARTITION BY adjuster_id ORDER BY claim_date_percent_rank ASC, id) AS prev_days_since_claim_date,
        FIRST_VALUE(claim_date_cume_dist) IGNORE NULLS OVER (ORDER BY days_since_claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date_cume_dist
    FROM annotated
)
-- Final projection
SELECT
    id,
    claim_date_dayofweek_percent_rank,
    claim_date_dayofweek_policy_id_min_claim_date_da,
    claim_date_month,
    claim_date_dense_rank,
    seq_no,
    next2_days_since_claim_date,
    prev_days_since_claim_date,
    first_known_claim_date_cume_dist,
    CASE WHEN claim_date_percent_rank >= 0.8 THEN 'large' WHEN claim_date_percent_rank >= 0.3 THEN 'medium' ELSE 'small' END AS claim_date_percent_rank_tier
FROM with_windows
ORDER BY claim_date_dayofweek_policy_id_min_claim_date_da
