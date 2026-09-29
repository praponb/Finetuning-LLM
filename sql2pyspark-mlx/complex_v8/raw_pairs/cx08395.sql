WITH prepared AS (
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
        CASE WHEN approved_amount >= 14668.0 THEN 'large' WHEN approved_amount >= 2887.3 THEN 'medium' ELSE 'small' END AS approved_amount_tier
    FROM claims
    WHERE claim_date IS NOT NULL
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        claim_type,
        approved_amount,
        reported_at,
        approved_amount_tier,
        FIRST_VALUE(reported_at) OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reported_at,
        FIRST_VALUE(approved_amount) OVER (ORDER BY reported_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_approved_amount,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY claim_type, policy_id), 2) AS approved_amount_vs_claim_type_policy_id_avg,
        DENSE_RANK() OVER (ORDER BY reported_at, id) AS reported_at_dense_rank,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount,
        MIN(approved_amount) OVER (ORDER BY reported_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_approved_amount_so_far,
        FIRST_VALUE(reported_at) OVER (PARTITION BY status, TO_DATE(reported_at) ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reported_at_2,
        LAG(claim_date, 2) OVER w_status_reported_at_to_date_b_2 AS prev2_claim_date,
        ROUND(AVG(approved_amount) OVER (PARTITION BY TO_DATE(reported_at)), 2) AS reported_at_to_date_avg_approved_amount,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY claim_type, YEAR(reported_at)), 0), 4) AS approved_amount_share_of_claim_type_reported_at_,
        DENSE_RANK() OVER (PARTITION BY approved_amount_tier, status ORDER BY claim_date DESC, id) AS claim_date_dense_rank,
        FIRST_VALUE(reported_at) OVER (PARTITION BY approved_amount_tier, status ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reported_at_3
    FROM prepared
    WINDOW w_claim_type_policy_id AS (PARTITION BY claim_type, policy_id), w_status_reported_at_to_date_b_2 AS (PARTITION BY status, TO_DATE(reported_at) ORDER BY approved_amount, id)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        policy_id,
        claim_type,
        approved_amount,
        reported_at,
        approved_amount_tier,
        first_reported_at,
        first_approved_amount,
        approved_amount_vs_claim_type_policy_id_avg,
        reported_at_dense_rank,
        last_known_approved_amount,
        min_approved_amount_so_far,
        first_reported_at_2,
        prev2_claim_date,
        reported_at_to_date_avg_approved_amount,
        last_known_reported_at,
        approved_amount_share_of_claim_type_reported_at_,
        first_reported_at_3
    FROM metrics
    WHERE claim_date_dense_rank = 1
      AND first_reported_at_2 >= TIMESTAMP '2025-02-06 09:00:00'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(first_approved_amount - AVG(first_approved_amount) OVER (PARTITION BY reported_at_dense_rank, policy_id), 2) AS first_approved_amount_vs_reported_at_dense_rank_,
        ROUND(AVG(last_known_approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', last_known_reported_at) ORDER BY first_reported_at_2 ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS last_known_approved_amount_avg_3_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', last_known_reported_at) ORDER BY first_reported_at_2 ASC), 3) AS first_reported_at_2_cume_dist,
        MAX(min_approved_amount_so_far) OVER w_reported_at_dense_rank_polic AS reported_at_dense_rank_policy_id_max_min_approve,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', last_known_reported_at) ORDER BY first_reported_at_2 ASC, id) AS first_reported_at_2_rank,
        LEAD(last_known_approved_amount, 1, 0) OVER (PARTITION BY DATE_TRUNC('MONTH', last_known_reported_at) ORDER BY first_reported_at_2 ASC, id) AS next_last_known_approved_amount,
        LAG(first_reported_at, 3) OVER (PARTITION BY DATE_TRUNC('MONTH', last_known_reported_at) ORDER BY first_reported_at_2 ASC, id) AS prev3_first_reported_at
    FROM top_per_group
    WINDOW w_reported_at_dense_rank_polic AS (PARTITION BY reported_at_dense_rank, policy_id)
)
SELECT
    id,
    claim_type,
    approved_amount,
    reported_at,
    first_reported_at,
    approved_amount_vs_claim_type_policy_id_avg,
    reported_at_dense_rank,
    min_approved_amount_so_far,
    last_known_reported_at,
    first_reported_at_3,
    first_approved_amount_vs_reported_at_dense_rank_,
    last_known_approved_amount_avg_3_prior,
    first_reported_at_2_cume_dist,
    reported_at_dense_rank_policy_id_max_min_approve,
    first_reported_at_2_rank,
    next_last_known_approved_amount
FROM with_trends
ORDER BY id DESC
