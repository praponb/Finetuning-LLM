WITH filtered AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE status = 'OPEN'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY MONTH(claim_date)), 0), 4) AS approved_amount_share_of_claim_date_month,
        LAG(claimed_amount) OVER (PARTITION BY policy_id ORDER BY approved_amount ASC, id) AS prev_claimed_amount,
        FIRST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY adjuster_id ORDER BY reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reported_at,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY claim_date ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_2_prior,
        COUNT(*) OVER w_claim_type_adjuster_id AS rows_in_claim_type_adjuster_id,
        MAX(approved_amount) OVER (PARTITION BY policy_id ORDER BY approved_amount ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS approved_amount_max_7,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY claim_type, adjuster_id)) AS distinct_policy_id_in_claim_type_adjuster_id,
        FIRST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY adjuster_id, policy_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY MONTH(claim_date)), 2) AS approved_amount_vs_claim_date_month_avg,
        MAX(approved_amount) OVER w_claim_type_adjuster_id AS claim_type_adjuster_id_max_approved_amount,
        SUM(approved_amount) OVER (PARTITION BY MONTH(claim_date)) AS claim_date_month_sum_approved_amount
    FROM filtered
    WINDOW w_claim_type_adjuster_id AS (PARTITION BY claim_type, adjuster_id)
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(claimed_amount_avg_2_prior / NULLIF(SUM(claimed_amount_avg_2_prior) OVER w_policy_id_claim_date_month, 0), 4) AS claimed_amount_avg_2_prior_share_of_policy_id_cl,
        ROUND(prev_claimed_amount - AVG(prev_claimed_amount) OVER (PARTITION BY policy_id, distinct_policy_id_in_claim_type_adjuster_id), 2) AS prev_claimed_amount_vs_policy_id_distinct_policy,
        MIN(approved_amount) OVER (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date)) AS policy_id_claim_date_month_min_approved_amount,
        NTILE(3) OVER w_distinct_policy_id_in_claim AS claim_type_adjuster_id_max_approved_amount_terci,
        NTILE(4) OVER w_all_by_claim_date_month_sum AS claim_date_month_sum_approved_amount_quartile
    FROM ranked
    WINDOW w_policy_id_claim_date_month AS (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date)), w_distinct_policy_id_in_claim AS (PARTITION BY distinct_policy_id_in_claim_type_adjuster_id, MONTH(first_known_reported_at) ORDER BY claim_type_adjuster_id_max_approved_amount, id), w_all_by_claim_date_month_sum AS (ORDER BY claim_date_month_sum_approved_amount DESC, id)
)
SELECT
    id,
    policy_id,
    approved_amount,
    reported_at,
    approved_amount_share_of_claim_date_month,
    claimed_amount_avg_2_prior,
    rows_in_claim_type_adjuster_id,
    approved_amount_max_7,
    distinct_policy_id_in_claim_type_adjuster_id,
    approved_amount_vs_claim_date_month_avg,
    claim_type_adjuster_id_max_approved_amount,
    claim_date_month_sum_approved_amount,
    claimed_amount_avg_2_prior_share_of_policy_id_cl,
    prev_claimed_amount_vs_policy_id_distinct_policy,
    claim_date_month_sum_approved_amount_quartile
FROM ranked_2
WHERE claim_date BETWEEN DATE '2024-04-09' AND DATE '2025-04-25'
ORDER BY prev_claimed_amount_vs_policy_id_distinct_policy DESC NULLS FIRST, distinct_policy_id_in_claim_type_adjuster_id
