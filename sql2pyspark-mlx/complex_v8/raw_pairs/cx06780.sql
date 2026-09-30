WITH joined_ranges AS (
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
scored AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(prior_claimed_amount_90d / 50000) * 50000 AS prior_claimed_amount_90d_bucket,
        FLOOR(prior_claimed_amount_90d / 50000) * 50000 AS prior_claimed_amount_90d_bucket_2
    FROM joined_ranges
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, prior_rows_90d, prior_claimed_amount_90d_bucket, prior_claimed_amount_90d_bucket_2
    FROM scored
) src
UNPIVOT (
    value FOR metric_name IN (prior_rows_90d, prior_claimed_amount_90d_bucket, prior_claimed_amount_90d_bucket_2)
) unp
ORDER BY id, metric_name
