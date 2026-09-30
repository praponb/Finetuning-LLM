WITH scoped_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at
    FROM claims
    WHERE approved_amount < 16404.6
      AND claim_type = 'delay'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status, policy_id ORDER BY claimed_amount), 3) AS claimed_amount_percent_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY claim_date DESC, id), 3) AS claim_date_percent_rank,
        SIZE(COLLECT_SET(policy_id) OVER w_status_claim_type) AS distinct_policy_id_in_status_claim_type,
        LAST_VALUE(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), policy_id ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_adjuster_id_policy_id, 2) AS claimed_amount_vs_adjuster_id_policy_id_avg,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY adjuster_id, policy_id)) AS distinct_status_in_adjuster_id_policy_id,
        COUNT(*) OVER (PARTITION BY status, adjuster_id) AS rows_in_status_adjuster_id,
        MAX(claimed_amount) OVER (PARTITION BY status, adjuster_id) AS status_adjuster_id_max_claimed_amount,
        MAX(claimed_amount) OVER w_adjuster_id_policy_id AS adjuster_id_policy_id_max_claimed_amount,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_status_claim_type, 2) AS claimed_amount_vs_status_claim_type_avg,
        LAG(claim_date) OVER (PARTITION BY status, policy_id ORDER BY claimed_amount, id) AS prev_claim_date,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY status, adjuster_id), 0), 4) AS claimed_amount_share_of_status_adjuster_id,
        ROW_NUMBER() OVER w_claim_date_month_policy_id_b_2 AS rn
    FROM scoped_claims
    WINDOW w_status_claim_type AS (PARTITION BY status, claim_type), w_adjuster_id_policy_id AS (PARTITION BY adjuster_id, policy_id), w_claim_date_month_policy_id_b_2 AS (PARTITION BY DATE_TRUNC('MONTH', claim_date), policy_id ORDER BY reported_at, id)
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        claimed_amount_percent_rank,
        claim_date_percent_rank,
        distinct_policy_id_in_status_claim_type,
        last_claimed_amount,
        claimed_amount_vs_adjuster_id_policy_id_avg,
        distinct_status_in_adjuster_id_policy_id,
        rows_in_status_adjuster_id,
        status_adjuster_id_max_claimed_amount,
        adjuster_id_policy_id_max_claimed_amount,
        claimed_amount_vs_status_claim_type_avg,
        prev_claim_date,
        claimed_amount_share_of_status_adjuster_id
    FROM windowed
    WHERE rn <= 3
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        claim_date,
        claimed_amount,
        claimed_amount_percent_rank,
        claim_date_percent_rank,
        distinct_policy_id_in_status_claim_type,
        last_claimed_amount,
        distinct_status_in_adjuster_id_policy_id,
        rows_in_status_adjuster_id,
        adjuster_id_policy_id_max_claimed_amount,
        claimed_amount_vs_status_claim_type_avg,
        prev_claim_date,
        ROUND(AVG(claim_date_percent_rank) OVER (ORDER BY status_adjuster_id_max_claimed_amount, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS claim_date_percent_rank_avg_2_prior,
        ROUND(PERCENT_RANK() OVER (ORDER BY claim_date, id), 3) AS claim_date_percent_rank_2,
        LEAD(claimed_amount) OVER (ORDER BY status_adjuster_id_max_claimed_amount, id) AS next_claimed_amount,
        NTH_VALUE(rows_in_status_adjuster_id, 2) OVER (ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rows_in_status_adjuster_id,
        ROUND(last_claimed_amount / NULLIF(SUM(last_claimed_amount) OVER w_distinct_policy_id_in_status, 0), 4) AS last_claimed_amount_share_of_distinct_policy_id_
    FROM leaders
    WINDOW w_distinct_policy_id_in_status AS (PARTITION BY distinct_policy_id_in_status_claim_type, distinct_status_in_adjuster_id_policy_id)
)
SELECT
    id,
    distinct_policy_id_in_status_claim_type,
    last_claimed_amount,
    distinct_status_in_adjuster_id_policy_id,
    adjuster_id_policy_id_max_claimed_amount,
    claimed_amount_vs_status_claim_type_avg,
    prev_claim_date,
    claim_date_percent_rank_2,
    next_claimed_amount,
    second_rows_in_status_adjuster_id,
    GREATEST(claimed_amount, claim_date_percent_rank) AS greatest_claimed_amount_claim_date_percent_rank,
    DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter
FROM metrics
