WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_180d,
        SUM(b.claimed_amount) AS prior_claimed_amount_180d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 180)
    GROUP BY a.id, a.policy_id, a.claim_date
),
stats AS (
    -- Summarise per group
    SELECT
        YEAR(claim_date) AS claim_date_year,
        policy_id,
        prior_rows_180d,
        SUM(prior_rows_180d) AS total_prior_rows_180d,
        MIN(prior_claimed_amount_180d) AS min_prior_claimed_amount_180d,
        ROUND(STDDEV(prior_rows_180d), 2) AS stddev_prior_rows_180d,
        ROUND(STDDEV(prior_claimed_amount_180d), 2) AS stddev_prior_claimed_amount_180d
    FROM joined_ranges
    WHERE prior_claimed_amount_180d < 175000.0
    GROUP BY YEAR(claim_date), policy_id, prior_rows_180d
    HAVING AVG(prior_claimed_amount_180d) > 87500.0
),
combined_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.claim_date_year,
        s.policy_id,
        s.prior_rows_180d,
        s.total_prior_rows_180d,
        s.min_prior_claimed_amount_180d,
        p.premium AS policy_premium,
        p.status AS policy_status,
        p.product AS policy_product,
        po.holder_name,
        po.region AS holder_region
    FROM stats s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
)
SELECT claim_date_year, policy_id, metric_name, metric_value
FROM (
    SELECT claim_date_year, policy_id, total_prior_rows_180d, prior_rows_180d
    FROM combined_details_policy
) src
UNPIVOT (
    metric_value FOR metric_name IN (total_prior_rows_180d, prior_rows_180d)
) unp
