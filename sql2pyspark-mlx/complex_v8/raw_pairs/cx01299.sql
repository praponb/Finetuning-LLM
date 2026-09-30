WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_90d,
        SUM(b.claimed_amount) AS prior_claimed_amount_90d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 90)
    GROUP BY a.id, a.policy_id, a.claim_date
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY prior_rows_90d, policy_id ORDER BY claim_date ASC), 3) AS claim_date_percent_rank,
        COUNT(*) OVER (PARTITION BY policy_id, prior_rows_90d) AS rows_in_policy_id_prior_rows_90d,
        ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY prior_claimed_amount_90d DESC, id) AS seq_no,
        RANK() OVER (PARTITION BY policy_id ORDER BY prior_claimed_amount_90d DESC) AS prior_claimed_amount_90d_rank,
        NTILE(4) OVER w_policy_id_by_prior_claimed_a AS prior_claimed_amount_90d_quartile,
        ROUND(prior_rows_90d / NULLIF(SUM(prior_rows_90d) OVER (PARTITION BY policy_id, prior_rows_90d), 0), 4) AS prior_rows_90d_share_of_policy_id_prior_rows_90d
    FROM in_window
    WINDOW w_policy_id_by_prior_claimed_a AS (PARTITION BY policy_id ORDER BY prior_claimed_amount_90d DESC, id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE prior_claimed_amount_90d_rank = 1
)
SELECT
    id,
    claim_date,
    prior_claimed_amount_90d,
    claim_date_percent_rank,
    rows_in_policy_id_prior_rows_90d,
    seq_no,
    prior_claimed_amount_90d_rank,
    prior_claimed_amount_90d_quartile,
    prior_rows_90d_share_of_policy_id_prior_rows_90d,
    ABS(seq_no - 7) AS seq_no_distance,
    DATE_FORMAT(claim_date, 'MMM yyyy') AS claim_date_label
FROM top_rows
