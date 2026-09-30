WITH prepared_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        reported_at,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM claims
    WHERE claim_date < DATE '2024-10-22'
      AND claimed_amount > 10518.4
),
with_details_policy AS (
    -- Join policies
    SELECT s.*, p.product AS policy_product
    FROM prepared_claims s
    LEFT JOIN policies p ON p.id = s.policy_id AND YEAR(p.end_date) = 2026
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_id,
        claimed_amount,
        reported_at,
        status_prefix,
        policy_product,
        MAX(claimed_amount) OVER (PARTITION BY policy_id, status) AS policy_id_status_max_claimed_amount,
        FIRST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY adjuster_id ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount,
        LAG(reported_at) OVER (PARTITION BY policy_id, policy_product ORDER BY reported_at, id) AS prev_reported_at,
        ROUND(PERCENT_RANK() OVER w_policy_id_policy_product_by_2, 3) AS reported_at_percent_rank,
        ROW_NUMBER() OVER (ORDER BY claimed_amount DESC, id) AS rn,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at))) AS distinct_policy_id_in_reported_at_month,
        MAX(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at)) AS reported_at_month_max_claimed_amount,
        claimed_amount - LAG(claimed_amount) OVER (ORDER BY claimed_amount DESC, id) AS claimed_amount_change,
        LEAD(claimed_amount, 2, 0) OVER (PARTITION BY adjuster_id ORDER BY reported_at DESC, id) AS next2_claimed_amount,
        DENSE_RANK() OVER (PARTITION BY policy_id, policy_product ORDER BY reported_at) AS reported_at_dense_rank,
        LAG(claimed_amount) OVER (ORDER BY claimed_amount DESC, id) AS prev_claimed_amount
    FROM with_details_policy
    WINDOW w_policy_id_policy_product_by_2 AS (PARTITION BY policy_id, policy_product ORDER BY reported_at)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE reported_at_dense_rank = 1
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claimed_amount,
    policy_product,
    first_known_claimed_amount,
    reported_at_percent_rank,
    rn,
    distinct_policy_id_in_reported_at_month,
    reported_at_month_max_claimed_amount,
    claimed_amount_change,
    reported_at_dense_rank,
    prev_claimed_amount
FROM leaders
ORDER BY prev_claimed_amount, id
LIMIT 20
