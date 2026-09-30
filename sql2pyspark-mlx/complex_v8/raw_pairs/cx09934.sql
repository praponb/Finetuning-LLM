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
        UPPER(TRIM(claim_type)) AS claim_type_clean
    FROM claims
    WHERE claim_date < DATE '2025-10-15'
      AND policy_id >= 17
),
enriched_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.reported_at,
        s.claim_type_clean,
        p.holder_id AS policy_holder_id,
        p.product AS policy_product
    FROM base_claims s
    JOIN policies p ON p.id = s.policy_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_id,
        claim_date,
        reported_at,
        claim_type_clean,
        policy_holder_id,
        LEAD(claim_date) OVER (PARTITION BY policy_id ORDER BY reported_at, id) AS next_claim_date,
        NTILE(5) OVER (PARTITION BY policy_product, policy_holder_id ORDER BY reported_at DESC NULLS FIRST, id) AS reported_at_quintile,
        NTILE(10) OVER (PARTITION BY adjuster_id ORDER BY claim_date NULLS LAST, id) AS claim_date_decile,
        FIRST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY policy_product, policy_holder_id ORDER BY reported_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reported_at,
        DENSE_RANK() OVER (PARTITION BY policy_product, policy_holder_id ORDER BY reported_at DESC NULLS FIRST, id) AS reported_at_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY adjuster_id ORDER BY claim_date NULLS LAST), 3) AS claim_date_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_product, policy_holder_id ORDER BY reported_at DESC NULLS FIRST, id), 3) AS reported_at_percent_rank,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY adjuster_id ORDER BY claim_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at,
        FIRST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reported_at_2,
        LAST_VALUE(reported_at) OVER (PARTITION BY policy_id ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at,
        FIRST_VALUE(claim_date) OVER (PARTITION BY policy_product, policy_holder_id ORDER BY reported_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date,
        ROUND(CUME_DIST() OVER w_policy_product_policy_holder, 3) AS reported_at_cume_dist,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY policy_id ORDER BY reported_at, id)) AS days_since_prev_claim_date
    FROM enriched_policy
    WINDOW w_policy_product_policy_holder AS (PARTITION BY policy_product, policy_holder_id ORDER BY reported_at DESC NULLS FIRST, id)
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(reported_at_dense_rank) OVER (PARTITION BY reported_at_dense_rank, policy_holder_id) AS reported_at_dense_rank_policy_holder_id_max_repo,
        ROUND(CUME_DIST() OVER (ORDER BY next_claim_date ASC), 3) AS next_claim_date_cume_dist,
        ROUND((UNIX_TIMESTAMP(last_known_reported_at) - UNIX_TIMESTAMP(LAG(last_known_reported_at) OVER w_policy_holder_id_by_next_cla)) / 60, 1) AS minutes_since_prev_last_known_reported_at,
        FIRST_VALUE(claim_date_cume_dist) OVER (ORDER BY next_claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date_cume_dist,
        MAX(claim_date_cume_dist) OVER w_reported_at_dense_rank_polic AS reported_at_dense_rank_policy_holder_id_max_clai,
        ROUND((UNIX_TIMESTAMP(last_reported_at) - UNIX_TIMESTAMP(LAG(last_reported_at) OVER (ORDER BY next_claim_date ASC, id))) / 60, 1) AS minutes_since_prev_last_reported_at,
        SIZE(COLLECT_SET(adjuster_id) OVER w_reported_at_dense_rank_polic) AS distinct_adjuster_id_in_reported_at_dense_rank_p
    FROM ranked
    WINDOW w_policy_holder_id_by_next_cla AS (PARTITION BY policy_holder_id ORDER BY next_claim_date DESC, id), w_reported_at_dense_rank_polic AS (PARTITION BY reported_at_dense_rank, policy_holder_id)
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_date,
    reported_at,
    claim_type_clean,
    next_claim_date,
    reported_at_quintile,
    claim_date_decile,
    reported_at_dense_rank,
    claim_date_cume_dist,
    reported_at_percent_rank,
    last_known_reported_at,
    first_known_reported_at_2,
    last_reported_at,
    reported_at_cume_dist,
    days_since_prev_claim_date,
    reported_at_dense_rank_policy_holder_id_max_repo,
    next_claim_date_cume_dist,
    minutes_since_prev_last_known_reported_at,
    first_claim_date_cume_dist,
    reported_at_dense_rank_policy_holder_id_max_clai,
    minutes_since_prev_last_reported_at,
    distinct_adjuster_id_in_reported_at_dense_rank_p
FROM ranked_2
WHERE claim_date IS NOT NULL
ORDER BY reported_at_cume_dist DESC NULLS FIRST
