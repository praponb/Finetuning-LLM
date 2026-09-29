WITH recent_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        status,
        reported_at,
        ADD_MONTHS(claim_date, 6) AS claim_date_plus_6m,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM claims
    WHERE claim_type IN ('medical', 'theft', 'delay')
      AND status NOT IN ('OPEN', 'APPROVED')
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount_filled,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY adjuster_id, status)) AS distinct_claim_type_in_adjuster_id_status,
        SUM(claimed_amount_filled) OVER w_adjuster_id_status AS adjuster_id_status_sum_claimed_amount_filled,
        ROUND(claimed_amount_filled / NULLIF(SUM(claimed_amount_filled) OVER (PARTITION BY adjuster_id, status), 0), 4) AS claimed_amount_filled_share_of_adjuster_id_statu,
        ROUND(claimed_amount_filled - AVG(claimed_amount_filled) OVER (PARTITION BY adjuster_id, status), 2) AS claimed_amount_filled_vs_adjuster_id_status_avg,
        SUM(claimed_amount_filled) OVER (PARTITION BY claim_type ORDER BY claimed_amount_filled, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS claimed_amount_filled_sum_2,
        RANK() OVER (PARTITION BY claim_type ORDER BY claimed_amount_filled) AS claimed_amount_filled_rank,
        LAG(claim_date_plus_6m, 2) OVER (PARTITION BY claim_type, adjuster_id ORDER BY claimed_amount_filled, id) AS prev2_claim_date_plus_6m,
        NTILE(4) OVER w_claim_type_adjuster_id_by_cl AS claimed_amount_filled_quartile,
        COUNT(*) OVER w_adjuster_id_status AS rows_in_adjuster_id_status,
        LAG(reported_at) OVER (PARTITION BY claim_type ORDER BY claimed_amount_filled, id) AS prev_reported_at,
        DENSE_RANK() OVER (PARTITION BY claim_type ORDER BY claimed_amount_filled) AS claimed_amount_filled_dense_rank
    FROM recent_claims
    WINDOW w_adjuster_id_status AS (PARTITION BY adjuster_id, status), w_claim_type_adjuster_id_by_cl AS (PARTITION BY claim_type, adjuster_id ORDER BY claimed_amount_filled, id)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        distinct_claim_type_in_adjuster_id_status,
        adjuster_id_status_sum_claimed_amount_filled,
        claimed_amount_filled_share_of_adjuster_id_statu,
        claimed_amount_filled_vs_adjuster_id_status_avg,
        claimed_amount_filled_sum_2,
        prev2_claim_date_plus_6m,
        claimed_amount_filled_quartile,
        rows_in_adjuster_id_status,
        claimed_amount_filled_dense_rank,
        DENSE_RANK() OVER (PARTITION BY claimed_amount_filled_rank, DATE_TRUNC('MONTH', prev2_claim_date_plus_6m) ORDER BY claimed_amount_filled_rank) AS claimed_amount_filled_rank_dense_rank,
        LAST_VALUE(claimed_amount_filled_sum_2) OVER (PARTITION BY YEAR(prev2_claim_date_plus_6m) ORDER BY claimed_amount_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount_filled_sum_2,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY claimed_amount_filled_rank)) AS distinct_adjuster_id_in_claimed_amount_filled_ra,
        SUM(claimed_amount_filled_rank) OVER (PARTITION BY adjuster_id, TO_DATE(prev_reported_at) ORDER BY claimed_amount_filled_dense_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount_filled_rank,
        ROUND(AVG(claimed_amount_filled_sum_2) OVER (PARTITION BY adjuster_id), 2) AS adjuster_id_avg_claimed_amount_filled_sum_2
    FROM ranked
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        claimed_amount_filled_rank_dense_rank,
        MIN(rows_in_adjuster_id_status) AS min_rows_in_adjuster_id_status,
        DATEDIFF(MAX(prev2_claim_date_plus_6m), MIN(prev2_claim_date_plus_6m)) AS prev2_claim_date_plus_6m_span_days
    FROM with_trends
    WHERE adjuster_id_avg_claimed_amount_filled_sum_2 < 97535.0
    GROUP BY 1
    HAVING MAX(distinct_adjuster_id_in_claimed_amount_filled_ra) > 2
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER w_claimed_amount_filled_rank_d AS prev2_claim_date_plus_6m_span_days_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY claimed_amount_filled_rank_dense_rank ORDER BY prev2_claim_date_plus_6m_span_days, claimed_amount_filled_rank_dense_rank), 3) AS prev2_claim_date_plus_6m_span_days_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY claimed_amount_filled_rank_dense_rank ORDER BY prev2_claim_date_plus_6m_span_days NULLS LAST, claimed_amount_filled_rank_dense_rank), 3) AS prev2_claim_date_plus_6m_span_days_cume_dist_2
    FROM stats
    WINDOW w_claimed_amount_filled_rank_d AS (PARTITION BY claimed_amount_filled_rank_dense_rank ORDER BY prev2_claim_date_plus_6m_span_days NULLS LAST)
)
SELECT
    claimed_amount_filled_rank_dense_rank,
    min_rows_in_adjuster_id_status,
    prev2_claim_date_plus_6m_span_days_rank,
    prev2_claim_date_plus_6m_span_days_cume_dist,
    prev2_claim_date_plus_6m_span_days_cume_dist_2
FROM metrics
WHERE prev2_claim_date_plus_6m_span_days BETWEEN 175 AND 525
ORDER BY prev2_claim_date_plus_6m_span_days_rank, min_rows_in_adjuster_id_status DESC, claimed_amount_filled_rank_dense_rank
LIMIT 5
