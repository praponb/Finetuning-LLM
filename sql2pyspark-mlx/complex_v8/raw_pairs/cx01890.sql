WITH matched AS (
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
derived AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        prior_rows_60d,
        prior_approved_amount_60d,
        DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label,
        ABS(prior_rows_60d - 6) AS prior_rows_60d_distance
    FROM matched
)
SELECT id, metric, reading
FROM (
    SELECT id, prior_rows_60d_distance, prior_rows_60d
    FROM derived
) src
UNPIVOT (
    reading FOR metric IN (prior_rows_60d_distance AS m_prior_rows_60d_distance, prior_rows_60d AS prior_rows_60d_v)
) unp
ORDER BY id, metric
