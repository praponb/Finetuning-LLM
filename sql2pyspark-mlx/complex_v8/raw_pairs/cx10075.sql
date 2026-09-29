WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY product ORDER BY premium DESC), 3) AS premium_cume_dist,
        ROUND(AVG(approved_amount) OVER w_adjuster_id_policy_id, 2) AS adjuster_id_policy_id_avg_approved_amount,
        ROUND(AVG(approved_amount) OVER (PARTITION BY product ORDER BY premium DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_1_prior,
        COUNT(*) OVER (PARTITION BY adjuster_id, policy_id) AS rows_in_adjuster_id_policy_id,
        ROUND(PERCENT_RANK() OVER w_product_by_premium, 3) AS premium_percent_rank,
        SUM(premium) OVER (PARTITION BY adjuster_id, policy_id) AS adjuster_id_policy_id_sum_premium
    FROM covered
    WINDOW w_adjuster_id_policy_id AS (PARTITION BY adjuster_id, policy_id), w_product_by_premium AS (PARTITION BY product ORDER BY premium DESC, id)
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.policy_id >= 13
              AND HOUR(c.reported_at) BETWEEN 8 AND 12
        )
),
with_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.adjuster_id,
        s.approved_amount,
        s.product,
        s.premium,
        s.premium_cume_dist,
        s.adjuster_id_policy_id_avg_approved_amount,
        s.approved_amount_avg_1_prior,
        s.rows_in_adjuster_id_policy_id,
        s.premium_percent_rank,
        s.adjuster_id_policy_id_sum_premium,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date,
        po.joined_on AS holder_joined_on,
        po.holder_name
    FROM screened s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT adjuster_id, policy_id, product, rows_in_adjuster_id_policy_id
    FROM with_details_policy
) src
PIVOT (
    AVG(rows_in_adjuster_id_policy_id)
    FOR product IN ('LIFE', 'HEALTH', 'TRAVEL')
)
