WITH cleaned_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        DATEDIFF(DATE '2025-12-31', reported_at) AS days_since_reported_at
    FROM claims
    WHERE policy_id IS NOT NULL
),
joined_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.days_since_reported_at,
        a.adjuster_name
    FROM cleaned_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id AND a.team IN ('Motor', 'Property')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_policy_id_claim_type_by_days, 3) AS days_since_reported_at_cume_dist,
        MAX(days_since_reported_at) OVER (ORDER BY claim_date DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS days_since_reported_at_max_2,
        NTH_VALUE(days_since_reported_at, 2) OVER (PARTITION BY policy_id, claim_type ORDER BY days_since_reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_reported_at,
        SUM(days_since_reported_at) OVER (PARTITION BY adjuster_id) AS adjuster_id_sum_days_since_reported_at,
        ROUND(AVG(days_since_reported_at) OVER (PARTITION BY adjuster_id), 2) AS adjuster_id_avg_days_since_reported_at,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY policy_id, claim_type ORDER BY days_since_reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        ROUND(days_since_reported_at - AVG(days_since_reported_at) OVER (PARTITION BY adjuster_id), 2) AS days_since_reported_at_vs_adjuster_id_avg,
        DATEDIFF(claim_date, LAG(claim_date) OVER (ORDER BY claim_date DESC, id)) AS days_since_prev_claim_date,
        DENSE_RANK() OVER w_claim_type_by_days_since_rep AS days_since_reported_at_dense_rank,
        COUNT(*) OVER (PARTITION BY policy_id, claim_type ORDER BY days_since_reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(PERCENT_RANK() OVER (ORDER BY claim_date DESC), 3) AS claim_date_percent_rank,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id)) AS distinct_policy_id_in_adjuster_id,
        LEAD(claim_date, 2) OVER (PARTITION BY claim_type ORDER BY days_since_reported_at, id) AS next2_claim_date,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY claim_type ORDER BY days_since_reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date_2
    FROM joined_adjuster
    WINDOW w_policy_id_claim_type_by_days AS (PARTITION BY policy_id, claim_type ORDER BY days_since_reported_at DESC), w_claim_type_by_days_since_rep AS (PARTITION BY claim_type ORDER BY days_since_reported_at)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(claim_date_percent_rank * 0.25, 2) AS claim_date_percent_rank_adj
    FROM sequenced
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        claim_date,
        days_since_reported_at,
        adjuster_name,
        days_since_reported_at_cume_dist,
        days_since_reported_at_max_2,
        second_days_since_reported_at,
        adjuster_id_avg_days_since_reported_at,
        first_known_claim_date,
        days_since_reported_at_vs_adjuster_id_avg,
        days_since_prev_claim_date,
        days_since_reported_at_dense_rank,
        running_count,
        claim_date_percent_rank,
        claim_date_percent_rank_adj,
        ROUND(AVG(days_since_prev_claim_date) OVER (PARTITION BY days_since_reported_at_dense_rank), 2) AS days_since_reported_at_dense_rank_avg_days_since,
        ROUND(days_since_reported_at_max_2 - AVG(days_since_reported_at_max_2) OVER (PARTITION BY days_since_reported_at_dense_rank), 2) AS days_since_reported_at_max_2_vs_days_since_repor,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_type ORDER BY days_since_reported_at_dense_rank DESC, id), 3) AS days_since_reported_at_dense_rank_cume_dist,
        FIRST_VALUE(first_known_claim_date_2) IGNORE NULLS OVER (ORDER BY days_since_reported_at_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_known_claim_date_2,
        LAST_VALUE(days_since_prev_claim_date) IGNORE NULLS OVER (ORDER BY days_since_reported_at_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_prev_claim_date,
        ROUND(CUME_DIST() OVER w_all_by_days_since_reported_a_2, 3) AS days_since_reported_at_max_2_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type ORDER BY days_since_reported_at_dense_rank DESC), 3) AS days_since_reported_at_dense_rank_percent_rank
    FROM tagged
    WINDOW w_all_by_days_since_reported_a_2 AS (ORDER BY days_since_reported_at_max_2)
)
-- Final projection
SELECT
    id,
    claim_date,
    days_since_reported_at,
    adjuster_name,
    days_since_reported_at_cume_dist,
    days_since_reported_at_max_2,
    second_days_since_reported_at,
    adjuster_id_avg_days_since_reported_at,
    days_since_reported_at_vs_adjuster_id_avg,
    days_since_prev_claim_date,
    running_count,
    claim_date_percent_rank,
    claim_date_percent_rank_adj,
    days_since_reported_at_dense_rank_avg_days_since,
    days_since_reported_at_max_2_vs_days_since_repor,
    first_known_first_known_claim_date_2,
    last_known_days_since_prev_claim_date,
    days_since_reported_at_max_2_cume_dist,
    days_since_reported_at_dense_rank_percent_rank,
    DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label,
    COALESCE(claim_date_percent_rank, 0.0) AS claim_date_percent_rank_filled
FROM sequenced_2
WHERE days_since_reported_at_cume_dist <= 0.9
ORDER BY days_since_reported_at_max_2
