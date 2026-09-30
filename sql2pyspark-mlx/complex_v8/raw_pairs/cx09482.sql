WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_90d,
        SUM(b.approved_amount) AS prior_approved_amount_90d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 90)
    GROUP BY a.id, a.policy_id, a.claim_date
),
unique_rows AS (
    -- Keep the latest row per policy_id
    SELECT
        id,
        policy_id,
        claim_date,
        prior_rows_90d,
        prior_approved_amount_90d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY prior_approved_amount_90d DESC, id DESC) AS row_num
        FROM joined_ranges
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_90d
FROM unique_rows
ORDER BY id
