WITH in_window AS (
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
joined_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_180d,
        s.prior_approved_amount_180d,
        p.end_date AS policy_end_date
    FROM in_window s
    LEFT JOIN policies p ON p.id = s.policy_id
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM joined_policy s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND HOUR(c.reported_at) BETWEEN 10 AND 15
              AND c.status <> 'APPROVED'
        )
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT SUM(c.approved_amount) FROM claims c WHERE c.policy_id = s.policy_id) AS total_claim_approved_amount
    FROM with_activity s
)
SELECT
    prior_rows_180d,
    policy_id,
    ROUND(STDDEV(prior_approved_amount_180d), 2) AS stddev_prior_approved_amount_180d,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_counts
GROUP BY ROLLUP(prior_rows_180d, policy_id)
ORDER BY prior_rows_180d NULLS LAST, policy_id NULLS LAST
