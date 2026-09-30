WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        CONCAT(claim_type, '#', CAST(id AS STRING)) AS claim_type_ref,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled
    FROM claims
    WHERE approved_amount >= 0.0
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        reported_at,
        claim_type_ref,
        approved_amount_filled,
        LAG(reported_at) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), claim_type ORDER BY approved_amount NULLS LAST, id) AS prev_reported_at,
        MIN(approved_amount) OVER (PARTITION BY claim_type) AS claim_type_min_approved_amount,
        LAST_VALUE(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), claim_type ORDER BY approved_amount NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        SUM(approved_amount) OVER (PARTITION BY claim_type) AS claim_type_sum_approved_amount,
        LAG(approved_amount) OVER (ORDER BY approved_amount DESC, id) AS prev_approved_amount,
        RANK() OVER w_all_by_approved_amount_ties AS approved_amount_rank
    FROM cleaned
    WINDOW w_all_by_approved_amount_ties AS (ORDER BY approved_amount DESC)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(last_claimed_amount) OVER w_policy_id AS policy_id_min_last_claimed_amount,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at), approved_amount_rank ORDER BY prev_reported_at ASC, id)) AS days_since_prev_claim_date,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_approved_amount_rank_policy) AS days_since_prev_claim_date_2,
        ROUND(CUME_DIST() OVER w_reported_at_month_approved_a_2, 3) AS prev_reported_at_cume_dist
    FROM with_windows
    WINDOW w_policy_id AS (PARTITION BY policy_id), w_approved_amount_rank_policy AS (PARTITION BY approved_amount_rank, policy_id ORDER BY prev_approved_amount, id), w_reported_at_month_approved_a_2 AS (PARTITION BY DATE_TRUNC('MONTH', reported_at), approved_amount_rank ORDER BY prev_reported_at ASC)
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(claim_type, 'ALL') AS claim_type,
    policy_id,
    GROUPING_ID() AS gid,
    SUM(policy_id_min_last_claimed_amount) FILTER (WHERE claim_type = 'fire') AS policy_id_min_last_claimed_amount_claim_type_fir,
    ROUND(AVG(days_since_prev_claim_date), 2) AS avg_days_since_prev_claim_date
FROM metrics
GROUP BY ROLLUP(claim_type, policy_id)
