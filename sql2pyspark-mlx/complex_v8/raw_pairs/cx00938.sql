WITH recent AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at,
        claimed_amount - approved_amount AS claimed_amount_minus_approved_amount
    FROM claims
    WHERE status IN ('IN_REVIEW', 'PAID', 'APPROVED')
      AND claimed_amount BETWEEN 15381.6 AND 43500.4
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.reported_at,
        p.status AS policy_status,
        p.start_date AS policy_start_date
    FROM recent s
    LEFT JOIN policies p ON p.id = s.policy_id AND p.premium BETWEEN 2033.8 AND 4906.3
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        policy_status,
        policy_start_date,
        NTILE(4) OVER (ORDER BY reported_at ASC NULLS LAST, id) AS reported_at_quartile,
        LAST_VALUE(claim_date) OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date,
        NTH_VALUE(policy_start_date, 2) OVER (PARTITION BY MONTH(reported_at) ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_policy_start_date,
        LEAD(claim_date, 2) OVER (ORDER BY policy_start_date ASC, id) AS next2_claim_date,
        LEAD(claim_date) OVER (PARTITION BY policy_id ORDER BY policy_start_date, id) AS next_claim_date,
        LEAD(claim_date) OVER w_policy_start_date_month_adju AS next_claim_date_2,
        RANK() OVER w_reported_at_month_by_claim_d_2 AS claim_date_rank,
        DENSE_RANK() OVER w_all_by_policy_start_date_tie AS policy_start_date_dense_rank
    FROM with_details
    WINDOW w_policy_start_date_month_adju AS (PARTITION BY DATE_TRUNC('MONTH', policy_start_date), adjuster_id ORDER BY reported_at, id), w_reported_at_month_by_claim_d_2 AS (PARTITION BY MONTH(reported_at) ORDER BY claim_date ASC, id), w_all_by_policy_start_date_tie AS (ORDER BY policy_start_date ASC)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(reported_at_quartile, 0) AS reported_at_quartile_filled,
        DATE_TRUNC('YEAR', next_claim_date) AS next_claim_date_year,
        FLOOR(reported_at_quartile / 2) * 2 AS reported_at_quartile_bucket
    FROM with_trends
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        policy_status,
        policy_start_date,
        last_claim_date,
        second_policy_start_date,
        next2_claim_date,
        next_claim_date_2,
        claim_date_rank,
        policy_start_date_dense_rank,
        reported_at_quartile_filled,
        reported_at_quartile_bucket,
        ROUND(claim_date_rank / NULLIF(SUM(claim_date_rank) OVER (PARTITION BY claim_date_rank, MONTH(next_claim_date_year)), 0), 4) AS claim_date_rank_share_of_claim_date_rank_next_cl,
        COUNT(*) OVER (PARTITION BY reported_at_quartile_filled, policy_start_date_dense_rank) AS rows_in_reported_at_quartile_filled_policy_start,
        RANK() OVER (PARTITION BY policy_start_date_dense_rank ORDER BY next_claim_date_year) AS next_claim_date_year_rank,
        ROUND(reported_at_quartile - AVG(reported_at_quartile) OVER (PARTITION BY reported_at_quartile_filled, policy_start_date_dense_rank), 2) AS reported_at_quartile_vs_reported_at_quartile_fil
    FROM derived
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    policy_status,
    policy_start_date,
    last_claim_date,
    second_policy_start_date,
    next2_claim_date,
    next_claim_date_2,
    claim_date_rank,
    policy_start_date_dense_rank,
    reported_at_quartile_filled,
    reported_at_quartile_bucket,
    claim_date_rank_share_of_claim_date_rank_next_cl,
    rows_in_reported_at_quartile_filled_policy_start,
    next_claim_date_year_rank,
    reported_at_quartile_vs_reported_at_quartile_fil,
    COALESCE(policy_status, 'UNKNOWN') AS policy_status_or_unknown,
    DATE_TRUNC('WEEK', second_policy_start_date) AS second_policy_start_date_week
FROM with_windows
ORDER BY policy_status NULLS LAST, last_claim_date, id
LIMIT 10
