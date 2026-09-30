WITH in_window AS (
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
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(prior_claimed_amount_60d, 2, 0) OVER (PARTITION BY YEAR(claim_date), prior_rows_60d ORDER BY prior_rows_60d NULLS LAST, id) AS next2_prior_claimed_amount_60d,
        LAST_VALUE(prior_claimed_amount_60d) OVER (PARTITION BY YEAR(claim_date), prior_rows_60d ORDER BY prior_rows_60d NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_prior_claimed_amount_60d,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY prior_rows_60d)) AS distinct_policy_id_in_prior_rows_60d,
        ROW_NUMBER() OVER (PARTITION BY policy_id, prior_rows_60d ORDER BY claim_date, id) AS seq_no,
        COUNT(*) OVER (PARTITION BY YEAR(claim_date), prior_rows_60d ORDER BY prior_rows_60d NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM in_window
)
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_60d,
    prior_claimed_amount_60d,
    next2_prior_claimed_amount_60d,
    last_prior_claimed_amount_60d,
    running_count
FROM ranked
ORDER BY prior_claimed_amount_60d DESC
