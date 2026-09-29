WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_30d,
        SUM(b.approved_amount) AS prior_approved_amount_30d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 30)
    GROUP BY a.id, a.policy_id, a.claim_date
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_date,
        prior_rows_30d,
        prior_approved_amount_30d,
        ROUND(prior_rows_30d / NULLIF(SUM(prior_rows_30d) OVER (PARTITION BY policy_id, prior_rows_30d), 0), 4) AS prior_rows_30d_share_of_policy_id_prior_rows_30d,
        ROUND(PERCENT_RANK() OVER w_all_by_prior_rows_30d, 3) AS prior_rows_30d_percent_rank
    FROM covered
    WINDOW w_all_by_prior_rows_30d AS (ORDER BY prior_rows_30d, id)
),
latest_per_key AS (
    -- Keep the earliest row per policy_id
    SELECT
        id,
        policy_id,
        claim_date,
        prior_rows_30d,
        prior_approved_amount_30d,
        prior_rows_30d_share_of_policy_id_prior_rows_30d,
        prior_rows_30d_percent_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY claim_date, id) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    policy_id,
    prior_approved_amount_30d,
    prior_rows_30d_share_of_policy_id_prior_rows_30d,
    prior_rows_30d_percent_rank
FROM latest_per_key
ORDER BY prior_rows_30d_share_of_policy_id_prior_rows_30d, id
LIMIT 5
