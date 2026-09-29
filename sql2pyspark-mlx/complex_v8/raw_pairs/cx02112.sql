WITH cleaned_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM claims
),
combined_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.status,
        s.reported_at,
        s.claim_type_or_unknown,
        p.end_date AS policy_end_date,
        p.product AS policy_product
    FROM cleaned_claims s
    JOIN policies p ON p.id = s.policy_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        status,
        reported_at,
        claim_type_or_unknown,
        policy_end_date,
        policy_product,
        FIRST_VALUE(policy_end_date) IGNORE NULLS OVER (PARTITION BY policy_id, policy_product ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_policy_end_date,
        NTILE(4) OVER (PARTITION BY adjuster_id, claim_type_or_unknown ORDER BY policy_end_date DESC NULLS FIRST, id) AS policy_end_date_quartile,
        RANK() OVER w_all_by_policy_end_date_ties AS policy_end_date_rank,
        RANK() OVER (PARTITION BY claim_type_or_unknown ORDER BY policy_end_date NULLS LAST, id) AS policy_end_date_rank_2,
        DENSE_RANK() OVER w_claim_type_or_unknown_policy AS reported_at_dense_rank,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY policy_id, policy_product ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        DENSE_RANK() OVER w_claim_type_or_unknown_policy_2 AS reported_at_dense_rank_2,
        LAG(policy_end_date) OVER (PARTITION BY status ORDER BY policy_end_date DESC, id) AS prev_policy_end_date,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_id, policy_product ORDER BY reported_at), 3) AS reported_at_cume_dist,
        RANK() OVER (PARTITION BY status ORDER BY policy_end_date DESC) AS policy_end_date_rank_3,
        LEAD(reported_at) OVER (PARTITION BY adjuster_id, claim_type_or_unknown ORDER BY policy_end_date DESC NULLS FIRST, id) AS next_reported_at,
        ROUND(CUME_DIST() OVER w_status_by_policy_end_date_ti, 3) AS policy_end_date_cume_dist,
        LAG(policy_end_date, 2) OVER (PARTITION BY policy_id, policy_product ORDER BY reported_at, id) AS prev2_policy_end_date,
        LEAD(policy_end_date) OVER (PARTITION BY policy_id, policy_product ORDER BY reported_at, id) AS next_policy_end_date
    FROM combined_details_policy
    WINDOW w_all_by_policy_end_date_ties AS (ORDER BY policy_end_date DESC NULLS FIRST), w_claim_type_or_unknown_policy AS (PARTITION BY claim_type_or_unknown, policy_id ORDER BY reported_at NULLS LAST, id), w_claim_type_or_unknown_policy_2 AS (PARTITION BY claim_type_or_unknown, policy_id ORDER BY reported_at NULLS LAST), w_status_by_policy_end_date_ti AS (PARTITION BY status ORDER BY policy_end_date DESC)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(reported_at_dense_rank, 0) AS reported_at_dense_rank_filled,
        DATE_FORMAT(next_reported_at, 'yyyy-MM-dd') AS next_reported_at_label,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM sequenced
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY claim_type_or_unknown, policy_end_date_quartile)) AS distinct_status_in_claim_type_or_unknown_policy_,
        MAX(policy_end_date_cume_dist) OVER (PARTITION BY claim_type_or_unknown, policy_end_date_quartile) AS claim_type_or_unknown_policy_end_date_quartile_m,
        ROUND(CUME_DIST() OVER (ORDER BY next_reported_at), 3) AS next_reported_at_cume_dist,
        LAST_VALUE(policy_end_date) OVER (ORDER BY prev2_policy_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_policy_end_date,
        ROUND(policy_end_date_rank_3 - AVG(policy_end_date_rank_3) OVER w_claim_type_or_unknown_policy_3, 2) AS policy_end_date_rank_3_vs_claim_type_or_unknown_,
        SIZE(COLLECT_SET(policy_product) OVER (PARTITION BY claim_type_or_unknown, policy_end_date_quartile)) AS distinct_policy_product_in_claim_type_or_unknown,
        DATEDIFF(policy_end_date, LAG(policy_end_date) OVER w_all_by_next_reported_at) AS days_since_prev_policy_end_date,
        LAST_VALUE(second_reported_at) IGNORE NULLS OVER (ORDER BY next_reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_second_reported_at
    FROM scored
    WINDOW w_claim_type_or_unknown_policy_3 AS (PARTITION BY claim_type_or_unknown, policy_end_date_quartile), w_all_by_next_reported_at AS (ORDER BY next_reported_at, id)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_policy_product_in_claim_type_or_unknown,
        ROUND(AVG(reported_at_dense_rank), 2) AS avg_reported_at_dense_rank,
        MAX(next_reported_at) AS last_next_reported_at
    FROM windowed
    WHERE claim_type_or_unknown <> 'delay'
    GROUP BY 1
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_policy_product_in_claim_type_or_unknown ORDER BY last_next_reported_at NULLS LAST, distinct_policy_product_in_claim_type_or_unknown), 3) AS last_next_reported_at_cume_dist,
        COUNT(*) OVER (PARTITION BY distinct_policy_product_in_claim_type_or_unknown) AS rows_in_distinct_policy_product_in_claim_type_or,
        ROW_NUMBER() OVER (PARTITION BY distinct_policy_product_in_claim_type_or_unknown ORDER BY last_next_reported_at NULLS LAST, distinct_policy_product_in_claim_type_or_unknown) AS rn
    FROM totals
    WINDOW w_distinct_policy_product_in_c_2 AS (PARTITION BY distinct_policy_product_in_claim_type_or_unknown)
)
SELECT
    distinct_policy_product_in_claim_type_or_unknown,
    last_next_reported_at,
    last_next_reported_at_cume_dist,
    rows_in_distinct_policy_product_in_claim_type_or,
    rn,
    DATE_FORMAT(last_next_reported_at, 'yyyy-MM-dd') AS last_next_reported_at_label,
    COALESCE(rn, 0) AS rn_filled
FROM ranked
WHERE rows_in_distinct_policy_product_in_claim_type_or <= 13
ORDER BY distinct_policy_product_in_claim_type_or_unknown NULLS LAST
