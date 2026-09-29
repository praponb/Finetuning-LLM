WITH joined_ranges AS (
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
annotated AS (
    -- Label and score rows
    SELECT
        *,
        prior_rows_60d + prior_claimed_amount_60d AS prior_rows_60d_plus_prior_claimed_amount_60d
    FROM joined_ranges
    WHERE prior_rows_60d > 1
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        prior_rows_60d,
        prior_claimed_amount_60d,
        ROUND(AVG(prior_rows_60d) OVER (PARTITION BY policy_id ORDER BY prior_rows_60d_plus_prior_claimed_amount_60d, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS prior_rows_60d_avg_1_prior,
        SUM(prior_rows_60d) OVER (PARTITION BY policy_id ORDER BY prior_rows_60d_plus_prior_claimed_amount_60d, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS prior_rows_60d_sum_3,
        ROUND(prior_rows_60d / NULLIF(SUM(prior_rows_60d) OVER (PARTITION BY prior_rows_60d), 0), 4) AS prior_rows_60d_share_of_prior_rows_60d,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY policy_id ORDER BY prior_rows_60d_plus_prior_claimed_amount_60d, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        FIRST_VALUE(prior_claimed_amount_60d) IGNORE NULLS OVER (PARTITION BY policy_id, prior_rows_60d ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prior_claimed_amount_60d,
        ROW_NUMBER() OVER w_policy_id_by_prior_rows_60d_4 AS seq_no
    FROM annotated
    WINDOW w_policy_id_by_prior_rows_60d_4 AS (PARTITION BY policy_id ORDER BY prior_rows_60d_plus_prior_claimed_amount_60d, id)
)
-- Final projection
SELECT
    id,
    policy_id,
    prior_rows_60d,
    prior_claimed_amount_60d,
    prior_rows_60d_avg_1_prior,
    prior_rows_60d_sum_3,
    prior_rows_60d_share_of_prior_rows_60d,
    first_known_prior_claimed_amount_60d,
    seq_no
FROM with_trends
