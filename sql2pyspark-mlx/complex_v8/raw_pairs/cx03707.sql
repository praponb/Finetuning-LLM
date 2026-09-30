WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
unique_rows AS (
    -- Keep the latest row per adjuster_id
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        status,
        adjuster_id,
        product,
        premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id ORDER BY claimed_amount DESC, id DESC) AS row_num
        FROM joined_ranges
    ) ranked
    WHERE row_num = 1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        status,
        adjuster_id,
        product,
        premium,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY policy_id, adjuster_id)) AS distinct_status_in_policy_id_adjuster_id,
        SUM(premium) OVER (ORDER BY premium DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS premium_sum_2_prior,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY policy_id, adjuster_id)) AS distinct_product_in_policy_id_adjuster_id
    FROM unique_rows
)
-- Subtotals with ROLLUP
SELECT
    adjuster_id,
    policy_id,
    SUM(claimed_amount) AS total_claimed_amount,
    COUNT(*) FILTER (WHERE product IS NOT NULL) AS n_product_is_not_null,
    COUNT(*) AS cnt
FROM windowed
GROUP BY ROLLUP(adjuster_id, policy_id)
ORDER BY adjuster_id NULLS LAST, policy_id NULLS LAST
