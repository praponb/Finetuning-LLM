WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_180d,
        SUM(b.approved_amount) AS prior_approved_amount_180d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 180)
    GROUP BY a.id, a.policy_id, a.claim_date
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(claim_date) OVER (PARTITION BY policy_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date,
        MAX(prior_approved_amount_180d) OVER w_policy_id_prior_rows_180d AS policy_id_prior_rows_180d_max_prior_approved_amo,
        DENSE_RANK() OVER w_policy_id_by_claim_date_ties AS claim_date_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_id ORDER BY claim_date), 3) AS claim_date_cume_dist,
        NTH_VALUE(prior_rows_180d, 2) OVER (PARTITION BY policy_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prior_rows_180d,
        LEAD(claim_date, 3) OVER w_policy_id_by_claim_date AS next3_claim_date
    FROM covered
    WINDOW w_policy_id_prior_rows_180d AS (PARTITION BY policy_id, prior_rows_180d), w_policy_id_by_claim_date_ties AS (PARTITION BY policy_id ORDER BY claim_date), w_policy_id_by_claim_date AS (PARTITION BY policy_id ORDER BY claim_date, id)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM with_trends
    WHERE claim_date_dense_rank = 1
),
with_parent AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_180d,
        s.prior_approved_amount_180d,
        s.last_claim_date,
        s.policy_id_prior_rows_180d_max_prior_approved_amo,
        s.claim_date_cume_dist,
        p.product AS policy_product,
        po.risk_score AS holder_risk_score,
        po.region AS holder_region
    FROM first_rows s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
    WHERE p.product NOT IN ('HEALTH', 'HOME')
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, reading
FROM (
    SELECT id, holder_risk_score, policy_id_prior_rows_180d_max_prior_approved_amo, claim_date_cume_dist, prior_approved_amount_180d
    FROM with_parent
) src
UNPIVOT (
    reading FOR metric_name IN (holder_risk_score AS m_holder_risk_score, policy_id_prior_rows_180d_max_prior_approved_amo AS policy_id_prior_rows_180d_max_prior_approved_amo_2, claim_date_cume_dist AS m_claim_date_cume_dist, prior_approved_amount_180d AS prior_approved_amount_180d_v)
) unp
ORDER BY id, metric_name
