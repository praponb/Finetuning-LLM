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
with_details AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_60d,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date,
        po.region AS holder_region
    FROM in_window s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(prior_rows_60d) OVER (PARTITION BY prior_rows_60d) AS prior_rows_60d_max_prior_rows_60d,
        MIN(prior_rows_60d) OVER (PARTITION BY prior_rows_60d) AS prior_rows_60d_min_prior_rows_60d,
        LAST_VALUE(prior_rows_60d) IGNORE NULLS OVER (ORDER BY policy_premium NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prior_rows_60d,
        ROUND(PERCENT_RANK() OVER (ORDER BY policy_premium NULLS LAST), 3) AS policy_premium_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', policy_start_date) ORDER BY policy_start_date, id) AS seq_no
    FROM with_details
)
SELECT
    id,
    claim_date,
    prior_rows_60d,
    policy_premium,
    policy_start_date,
    holder_region,
    prior_rows_60d_max_prior_rows_60d,
    prior_rows_60d_min_prior_rows_60d,
    last_known_prior_rows_60d,
    policy_premium_percent_rank,
    COALESCE(holder_region, 'UNKNOWN') AS holder_region_or_unknown,
    UPPER(TRIM(holder_region)) AS holder_region_clean
FROM with_windows
ORDER BY policy_premium, holder_region_or_unknown DESC NULLS FIRST
