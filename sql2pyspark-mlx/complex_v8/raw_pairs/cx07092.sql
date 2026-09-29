WITH base_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        reported_at,
        CONCAT(claim_type, '#', CAST(adjuster_id AS STRING)) AS claim_type_ref,
        claimed_amount + approved_amount AS claimed_amount_plus_approved_amount
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 10 AND 13
      AND claim_date < DATE '2024-08-20'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY claimed_amount_plus_approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY approved_amount, id) AS rn,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at,
        COUNT(*) OVER w_status_adjuster_id AS rows_in_status_adjuster_id,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_status_adjuster_id, 0), 4) AS approved_amount_share_of_status_adjuster_id,
        SIZE(COLLECT_SET(adjuster_id) OVER w_status) AS distinct_adjuster_id_in_status,
        NTH_VALUE(claim_date, 2) OVER (ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        SUM(approved_amount) OVER w_status_adjuster_id AS status_adjuster_id_sum_approved_amount,
        ROUND(approved_amount - AVG(approved_amount) OVER w_status_adjuster_id, 2) AS approved_amount_vs_status_adjuster_id_avg,
        LAG(claim_date, 2) OVER w_all_by_claim_date AS prev2_claim_date,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY status), 2) AS approved_amount_vs_status_avg,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY approved_amount) AS approved_amount_dense_rank,
        DENSE_RANK() OVER w_policy_id_by_claimed_amount_2 AS claimed_amount_plus_approved_amount_dense_rank,
        RANK() OVER w_policy_id_by_claim_date_ties AS claim_date_rank,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id ORDER BY claim_date, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_1_prior
    FROM base_claims
    WINDOW w_status_adjuster_id AS (PARTITION BY status, adjuster_id), w_status AS (PARTITION BY status), w_all_by_claim_date AS (ORDER BY claim_date ASC, id), w_policy_id_by_claimed_amount_2 AS (PARTITION BY policy_id ORDER BY claimed_amount_plus_approved_amount DESC), w_policy_id_by_claim_date_ties AS (PARTITION BY policy_id ORDER BY claim_date)
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    approved_amount,
    status,
    claim_type_ref,
    last_known_approved_amount,
    last_known_reported_at,
    approved_amount_share_of_status_adjuster_id,
    distinct_adjuster_id_in_status,
    approved_amount_vs_status_adjuster_id_avg,
    prev2_claim_date,
    approved_amount_vs_status_avg,
    claimed_amount_plus_approved_amount_dense_rank,
    claim_date_rank,
    approved_amount_avg_1_prior,
    LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
FROM with_trends
ORDER BY approved_amount_vs_status_avg, last_known_reported_at DESC
