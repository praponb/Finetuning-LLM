WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        LAST_DAY(claim_date) AS claim_date_month_end
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.claim_date_month_end,
        a.level AS adjuster_level,
        a.adjuster_name,
        p.product AS policy_product
    FROM cleaned s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.team IN ('Health', 'Travel', 'Property')
    LEFT JOIN policies p ON p.id = s.policy_id
    WHERE LENGTH(TRIM(a.adjuster_name)) >= 4
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(claimed_amount) OVER (PARTITION BY policy_product) AS policy_product_sum_claimed_amount,
        LEAD(claim_date_month_end) OVER (PARTITION BY adjuster_id ORDER BY adjuster_level NULLS LAST, id) AS next_claim_date_month_end,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY policy_product, adjuster_id ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount,
        DENSE_RANK() OVER w_policy_product_by_claim_date AS claim_date_dense_rank,
        SIZE(COLLECT_SET(status) OVER w_policy_product) AS distinct_status_in_policy_product,
        ROW_NUMBER() OVER (PARTITION BY policy_product ORDER BY claim_date DESC, id) AS seq_no,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY policy_product ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        COUNT(*) OVER w_policy_product AS rows_in_policy_product,
        ROUND(adjuster_level - AVG(adjuster_level) OVER w_policy_product, 2) AS adjuster_level_vs_policy_product_avg,
        NTH_VALUE(claimed_amount, 2) OVER (PARTITION BY policy_product, adjuster_id ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        DENSE_RANK() OVER (PARTITION BY adjuster_id ORDER BY adjuster_level NULLS LAST, id) AS adjuster_level_dense_rank
    FROM with_details
    WINDOW w_policy_product_by_claim_date AS (PARTITION BY policy_product ORDER BY claim_date DESC), w_policy_product AS (PARTITION BY policy_product)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix,
        ABS(rows_in_policy_product - 10) AS rows_in_policy_product_distance,
        SUBSTRING(TRIM(adjuster_name), 1, 3) AS adjuster_name_prefix
    FROM with_windows
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    status,
    reported_at,
    claim_date_month_end,
    adjuster_level,
    adjuster_name,
    policy_product,
    policy_product_sum_claimed_amount,
    last_known_claimed_amount,
    claim_date_dense_rank,
    distinct_status_in_policy_product,
    seq_no,
    second_claim_date,
    rows_in_policy_product,
    adjuster_level_vs_policy_product_avg,
    second_claimed_amount,
    adjuster_level_dense_rank,
    status_prefix,
    adjuster_name_prefix
FROM tagged
WHERE rows_in_policy_product < 14
ORDER BY claim_date_month_end DESC NULLS FIRST, id NULLS LAST
