WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_60d,
        SUM(b.approved_amount) AS prior_approved_amount_60d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 60)
    GROUP BY a.id, a.policy_id, a.claim_date
),
joined_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.prior_rows_60d,
        s.prior_approved_amount_60d,
        p.premium AS policy_premium,
        p.product AS policy_product
    FROM in_window s
    JOIN policies p ON p.id = s.policy_id AND p.start_date >= DATE '2024-08-27'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(prior_rows_60d) OVER (PARTITION BY policy_product) AS policy_product_max_prior_rows_60d,
        DENSE_RANK() OVER (PARTITION BY prior_rows_60d ORDER BY policy_premium DESC) AS policy_premium_dense_rank,
        ROUND(AVG(policy_premium) OVER (PARTITION BY policy_product), 2) AS policy_product_avg_policy_premium
    FROM joined_policy
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(policy_premium * 1.1, 2) AS policy_premium_adj,
        ABS(prior_rows_60d - 4) AS prior_rows_60d_distance
    FROM windowed
)
SELECT id, measure, metric_value
FROM (
    SELECT id, policy_premium_dense_rank, prior_rows_60d, prior_rows_60d_distance
    FROM derived
) src
UNPIVOT (
    metric_value FOR measure IN (policy_premium_dense_rank AS m_policy_premium_dense_rank, prior_rows_60d AS prior_rows_60d_v, prior_rows_60d_distance AS prior_rows_60d_distance_v)
) unp
