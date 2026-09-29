WITH covered AS (
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
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date,
        CASE WHEN prior_rows_60d >= 5 THEN 'A' WHEN prior_rows_60d >= 3 THEN 'B' ELSE 'C' END AS prior_rows_60d_tier,
        ABS(prior_rows_60d - 4) AS prior_rows_60d_distance
    FROM covered
    WHERE prior_rows_60d < 4
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        claim_date,
        prior_rows_60d,
        prior_approved_amount_60d,
        days_since_claim_date,
        prior_rows_60d_distance,
        FLOOR(days_since_claim_date / 100) * 100 AS days_since_claim_date_bucket
    FROM annotated
    WHERE prior_rows_60d_distance <= 5
),
with_details_policy AS (
    -- Join policies
    SELECT s.*, p.product AS policy_product, po.holder_name
    FROM tagged s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
)
SELECT
    id,
    policy_id,
    claim_date,
    prior_rows_60d,
    prior_approved_amount_60d,
    days_since_claim_date,
    prior_rows_60d_distance,
    days_since_claim_date_bucket,
    holder_name
FROM with_details_policy
ORDER BY prior_rows_60d_distance DESC
