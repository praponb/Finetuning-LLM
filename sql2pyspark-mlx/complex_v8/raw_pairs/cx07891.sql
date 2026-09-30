WITH in_window AS (
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
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(prior_claimed_amount_180d - AVG(prior_claimed_amount_180d) OVER w_policy_id, 2) AS prior_claimed_amount_180d_vs_policy_id_avg,
        ROUND(prior_claimed_amount_180d / NULLIF(SUM(prior_claimed_amount_180d) OVER (PARTITION BY policy_id), 0), 4) AS prior_claimed_amount_180d_share_of_policy_id,
        ROUND(AVG(prior_rows_180d) OVER (PARTITION BY policy_id), 2) AS policy_id_avg_prior_rows_180d,
        ROUND(CUME_DIST() OVER (ORDER BY prior_rows_180d DESC), 3) AS prior_rows_180d_cume_dist,
        DATEDIFF(claim_date, LAG(claim_date) OVER (ORDER BY prior_rows_180d, id)) AS days_since_prev_claim_date
    FROM in_window
    WINDOW w_policy_id AS (PARTITION BY policy_id)
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
        )
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_activity s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claim_date IS NOT NULL
        )
)
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_180d,
    prior_claimed_amount_180d,
    prior_claimed_amount_180d_vs_policy_id_avg,
    prior_claimed_amount_180d_share_of_policy_id,
    policy_id_avg_prior_rows_180d,
    prior_rows_180d_cume_dist,
    days_since_prev_claim_date,
    DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_label
FROM without_matches
ORDER BY policy_id_avg_prior_rows_180d DESC, claim_date DESC NULLS FIRST
