WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        ROUND(approved_amount * 0.9, 2) AS approved_amount_adj
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
with_details AS (
    -- Join policies
    SELECT s.*, p.product AS policy_product
    FROM base s
    JOIN policies p ON p.id = s.policy_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(approved_amount_adj) OVER (PARTITION BY policy_product, adjuster_id ORDER BY reported_at DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS approved_amount_adj_avg_3,
        ROUND(CUME_DIST() OVER w_policy_product_by_approved_a, 3) AS approved_amount_cume_dist,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY policy_product ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        ROW_NUMBER() OVER (PARTITION BY policy_product, adjuster_id ORDER BY reported_at DESC NULLS FIRST, id) AS rn,
        ROUND(PERCENT_RANK() OVER (ORDER BY approved_amount NULLS LAST, id), 3) AS approved_amount_percent_rank,
        DENSE_RANK() OVER (ORDER BY approved_amount NULLS LAST) AS approved_amount_dense_rank,
        MAX(approved_amount_adj) OVER (PARTITION BY policy_product, adjuster_id ORDER BY approved_amount_adj, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS approved_amount_adj_max_2_prior,
        DENSE_RANK() OVER (PARTITION BY policy_product, adjuster_id ORDER BY approved_amount_adj) AS approved_amount_adj_dense_rank,
        ROW_NUMBER() OVER w_all_by_approved_amount AS approved_amount_seq
    FROM with_details
    WINDOW w_policy_product_by_approved_a AS (PARTITION BY policy_product ORDER BY approved_amount DESC), w_all_by_approved_amount AS (ORDER BY approved_amount NULLS LAST, id)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(approved_amount * 1.07, 2) AS approved_amount_adj_2,
        LEAST(rn, approved_amount) AS least_rn_approved_amount
    FROM windowed
),
per_group AS (
    -- Group-level totals
    SELECT
        TO_DATE(reported_at) AS reported_at_day,
        rn,
        ROUND(STDDEV(approved_amount_adj), 2) AS stddev_approved_amount_adj,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
        MAX(approved_amount_seq) AS max_approved_amount_seq,
        ROUND(AVG(approved_amount_percent_rank), 2) AS avg_approved_amount_percent_rank,
        COUNT(DISTINCT claim_type) AS distinct_claim_types
    FROM derived
    GROUP BY TO_DATE(reported_at), rn
    HAVING AVG(approved_amount_seq) > 3
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_distinct_claim_types_distinc AS rows_in_distinct_claim_types_distinct_adjuster_i,
        NTH_VALUE(stddev_approved_amount_adj, 2) OVER (PARTITION BY distinct_adjuster_ids, rn ORDER BY max_approved_amount_seq NULLS LAST, reported_at_day, rn ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_stddev_approved_amount_adj,
        distinct_claim_types - LAG(distinct_claim_types) OVER w_all_by_distinct_claim_types AS distinct_claim_types_change
    FROM per_group
    WINDOW w_distinct_claim_types_distinc AS (PARTITION BY distinct_claim_types, distinct_adjuster_ids), w_all_by_distinct_claim_types AS (ORDER BY distinct_claim_types, reported_at_day, rn)
)
SELECT
    reported_at_day,
    rn,
    stddev_approved_amount_adj,
    distinct_adjuster_ids,
    max_approved_amount_seq,
    distinct_claim_types,
    rows_in_distinct_claim_types_distinct_adjuster_i,
    second_stddev_approved_amount_adj,
    distinct_claim_types_change,
    FLOOR(distinct_claim_types / 2) * 2 AS distinct_claim_types_bucket
FROM metrics
ORDER BY rn
