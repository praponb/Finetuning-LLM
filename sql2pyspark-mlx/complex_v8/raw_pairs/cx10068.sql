WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE approved_amount <= 32878.5
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        status,
        reported_at,
        LEAD(reported_at) OVER (PARTITION BY TO_DATE(reported_at), claim_type ORDER BY claim_date, id) AS next_reported_at,
        ROUND(PERCENT_RANK() OVER w_reported_at_to_date_claim_ty_2, 3) AS claim_date_percent_rank,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY policy_id)) AS distinct_adjuster_id_in_policy_id,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY TO_DATE(reported_at)), 2) AS approved_amount_vs_reported_at_to_date_avg,
        ROUND(AVG(approved_amount) OVER (PARTITION BY adjuster_id, policy_id ORDER BY approved_amount DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_2_prior,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY TO_DATE(reported_at))) AS distinct_policy_id_in_reported_at_to_date,
        DENSE_RANK() OVER (PARTITION BY policy_id, claim_type ORDER BY reported_at DESC) AS reported_at_dense_rank,
        LAST_VALUE(reported_at) OVER (PARTITION BY adjuster_id, policy_id ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at
    FROM valid
    WINDOW w_reported_at_to_date_claim_ty_2 AS (PARTITION BY TO_DATE(reported_at), claim_type ORDER BY claim_date)
),
leaders AS (
    -- Top 2 per group
    SELECT *
    FROM windowed
    WHERE reported_at_dense_rank <= 2
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        status,
        next_reported_at,
        claim_date_percent_rank,
        distinct_adjuster_id_in_policy_id,
        approved_amount_vs_reported_at_to_date_avg,
        distinct_policy_id_in_reported_at_to_date,
        reported_at_dense_rank,
        last_reported_at,
        SIZE(COLLECT_SET(adjuster_id) OVER w_distinct_adjuster_id_in_poli) AS distinct_adjuster_id_in_distinct_adjuster_id_in_,
        SUM(reported_at_dense_rank) OVER (PARTITION BY distinct_adjuster_id_in_policy_id) AS distinct_adjuster_id_in_policy_id_sum_reported_a,
        LAG(claim_date_percent_rank, 2) OVER w_reported_at_month_distinct_a AS prev2_claim_date_percent_rank,
        NTILE(4) OVER (PARTITION BY TO_DATE(reported_at), distinct_policy_id_in_reported_at_to_date ORDER BY reported_at, id) AS reported_at_quartile,
        LAG(approved_amount_avg_2_prior, 3) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at), distinct_adjuster_id_in_policy_id ORDER BY reported_at DESC NULLS FIRST, id) AS prev3_approved_amount_avg_2_prior,
        RANK() OVER (PARTITION BY TO_DATE(reported_at), distinct_policy_id_in_reported_at_to_date ORDER BY reported_at) AS reported_at_rank
    FROM leaders
    WINDOW w_distinct_adjuster_id_in_poli AS (PARTITION BY distinct_adjuster_id_in_policy_id), w_reported_at_month_distinct_a AS (PARTITION BY DATE_TRUNC('MONTH', reported_at), distinct_adjuster_id_in_policy_id ORDER BY reported_at DESC NULLS FIRST, id)
),
stats AS (
    -- Group-level totals
    SELECT
        reported_at_dense_rank,
        reported_at_rank,
        policy_id,
        SUM(prev2_claim_date_percent_rank) FILTER (WHERE status = 'REJECTED') AS prev2_claim_date_percent_rank_status_rejected,
        COUNT(*) AS row_count,
        SUM(prev2_claim_date_percent_rank) AS total_prev2_claim_date_percent_rank
    FROM ranked
    WHERE last_reported_at >= TIMESTAMP '2025-01-25 11:00:00'
    GROUP BY reported_at_dense_rank, reported_at_rank, policy_id
    HAVING MAX(reported_at_dense_rank) > 2
)
SELECT
    reported_at_dense_rank,
    reported_at_rank,
    policy_id,
    row_count,
    COALESCE(reported_at_rank, 0) AS reported_at_rank_filled
FROM stats
ORDER BY policy_id
