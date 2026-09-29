WITH cleaned AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at
    FROM claims
    WHERE claimed_amount > 3703.0
      AND approved_amount <= 21750.2
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_policy_id_claim_date_month AS rows_in_policy_id_claim_date_month,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER w_adjuster_id_by_claimed_amoun)) / 60, 1) AS minutes_since_prev_reported_at,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_policy_id_claim_date_month, 0), 4) AS claimed_amount_share_of_policy_id_claim_date_mon,
        DENSE_RANK() OVER (ORDER BY claim_date DESC) AS claim_date_dense_rank,
        MAX(claimed_amount) OVER (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date)) AS policy_id_claim_date_month_max_claimed_amount,
        LAG(claimed_amount, 1, 0) OVER (ORDER BY claim_date DESC, id) AS prev_claimed_amount,
        COUNT(*) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAG(claim_date) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount DESC, id) AS prev_claim_date,
        ROUND(CUME_DIST() OVER w_all_by_claim_date_ties, 3) AS claim_date_cume_dist,
        RANK() OVER (PARTITION BY adjuster_id ORDER BY claim_date ASC, id) AS claim_date_rank,
        NTH_VALUE(claimed_amount, 2) OVER (ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_7,
        RANK() OVER (ORDER BY claim_date DESC) AS claim_date_rank_2
    FROM cleaned
    WINDOW w_policy_id_claim_date_month AS (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date)), w_adjuster_id_by_claimed_amoun AS (PARTITION BY adjuster_id ORDER BY claimed_amount DESC, id), w_all_by_claim_date_ties AS (ORDER BY claim_date DESC)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE claim_date_dense_rank = 1
),
with_windows_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        claim_date_dense_rank,
        prev_claimed_amount,
        running_count,
        claim_date_rank,
        second_claimed_amount,
        claimed_amount_avg_7,
        claim_date_rank_2,
        NTILE(10) OVER (ORDER BY claim_date_cume_dist ASC, id) AS claim_date_cume_dist_decile,
        claim_date_rank - LAG(claim_date_rank) OVER w_all_by_claim_date_2 AS claim_date_rank_change,
        LEAD(claim_date_cume_dist, 1, 0) OVER (ORDER BY claim_date, id) AS next_claim_date_cume_dist,
        SUM(prev_claimed_amount) OVER (ORDER BY claim_date_cume_dist ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS prev_claimed_amount_sum_2_prior,
        ROUND(PERCENT_RANK() OVER (ORDER BY claim_date_cume_dist ASC), 3) AS claim_date_cume_dist_percent_rank,
        RANK() OVER (ORDER BY claim_date_cume_dist ASC, id) AS claim_date_cume_dist_rank
    FROM top_rows
    WINDOW w_all_by_claim_date_2 AS (ORDER BY claim_date, id)
),
stats AS (
    -- Summarise per group
    SELECT
        adjuster_id,
        DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days,
        MIN(claim_date) AS first_claim_date,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(claim_date_rank_change, 0.5) AS median_claim_date_rank_change
    FROM with_windows_2
    GROUP BY adjuster_id
    HAVING SUM(claimed_amount_avg_7) > 17565.0
)
SELECT
    adjuster_id,
    first_claim_date,
    distinct_ids,
    median_claim_date_rank_change,
    CASE WHEN claim_date_span_days >= 560 THEN 'HIGH' WHEN claim_date_span_days >= 245 THEN 'MEDIUM' ELSE 'LOW' END AS claim_date_span_days_tier
FROM stats
ORDER BY adjuster_id
LIMIT 10
