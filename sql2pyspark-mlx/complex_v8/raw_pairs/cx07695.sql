WITH in_window AS (
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
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
adjuster_id_stats AS (
    -- Median premium per adjuster_id
    SELECT adjuster_id, PERCENTILE_APPROX(premium, 0.5) AS median_premium
    FROM in_window
    GROUP BY adjuster_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_premium,
        ROUND(s.premium - g.median_premium, 2) AS premium_minus_median
    FROM in_window s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.premium >= g.median_premium
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(approved_amount) OVER (PARTITION BY adjuster_id ORDER BY approved_amount, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_1_prior,
        LAG(premium, 1, 0) OVER (PARTITION BY adjuster_id ORDER BY approved_amount, id) AS prev_premium,
        ROUND(AVG(premium) OVER (PARTITION BY adjuster_id ORDER BY approved_amount, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS premium_avg_2,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id)) AS distinct_policy_id_in_adjuster_id,
        ROUND(premium_minus_median - AVG(premium_minus_median) OVER w_adjuster_id, 2) AS premium_minus_median_vs_adjuster_id_avg
    FROM outliers
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id)
),
with_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claimed_amount,
        s.adjuster_id,
        s.approved_amount,
        s.product,
        s.premium,
        s.median_premium,
        s.premium_minus_median,
        s.prev_premium,
        s.premium_avg_2,
        s.distinct_policy_id_in_adjuster_id,
        s.premium_minus_median_vs_adjuster_id_avg,
        p.start_date AS policy_start_date,
        po.risk_score AS holder_risk_score
    FROM sequenced s
    JOIN policies p ON p.id = s.policy_id AND p.start_date < DATE '2025-02-14'
    LEFT JOIN policyholders po ON po.id = p.holder_id
    WHERE p.start_date IS NOT NULL
)
SELECT id, measure, value
FROM (
    SELECT id, premium_avg_2, premium_minus_median_vs_adjuster_id_avg, median_premium, holder_risk_score
    FROM with_details_policy
) src
UNPIVOT (
    value FOR measure IN (premium_avg_2 AS m_premium_avg_2, premium_minus_median_vs_adjuster_id_avg AS premium_minus_median_vs_adjuster_id_avg_v, median_premium AS m_median_premium, holder_risk_score AS holder_risk_score_v)
) unp
