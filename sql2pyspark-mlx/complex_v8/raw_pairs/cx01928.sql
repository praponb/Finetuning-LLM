WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_60d,
        SUM(b.claimed_amount) AS prior_claimed_amount_60d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 60)
    GROUP BY a.id, a.policy_id, a.claim_date
),
with_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_60d,
        s.prior_claimed_amount_60d,
        p.start_date AS policy_start_date,
        p.end_date AS policy_end_date
    FROM matched s
    LEFT JOIN policies p ON p.id = s.policy_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        claim_date,
        prior_rows_60d,
        prior_claimed_amount_60d,
        COUNT(*) OVER (PARTITION BY YEAR(policy_start_date) ORDER BY prior_rows_60d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DATEDIFF(policy_start_date, LAG(policy_start_date) OVER (ORDER BY prior_claimed_amount_60d, id)) AS days_since_prev_policy_start_date,
        DATEDIFF(claim_date, LAG(claim_date) OVER (ORDER BY prior_claimed_amount_60d, id)) AS days_since_prev_claim_date,
        NTILE(10) OVER (PARTITION BY YEAR(policy_start_date) ORDER BY prior_rows_60d, id) AS prior_rows_60d_decile
    FROM with_details_policy
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        claim_date,
        prior_claimed_amount_60d,
        days_since_prev_policy_start_date,
        days_since_prev_claim_date,
        prior_rows_60d_decile,
        FLOOR(prior_claimed_amount_60d / 50000) * 50000 AS prior_claimed_amount_60d_bucket,
        ABS(prior_claimed_amount_60d - 175000.0) AS prior_claimed_amount_60d_distance,
        COALESCE(prior_rows_60d_decile, 0) AS prior_rows_60d_decile_filled
    FROM with_trends
)
SELECT
    id,
    claim_date,
    prior_claimed_amount_60d,
    days_since_prev_policy_start_date,
    days_since_prev_claim_date,
    prior_rows_60d_decile,
    prior_claimed_amount_60d_bucket,
    prior_claimed_amount_60d_distance,
    COALESCE(prior_claimed_amount_60d, 0.0) AS prior_claimed_amount_60d_filled
FROM tagged
ORDER BY days_since_prev_policy_start_date
