WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
    FROM claims
    WHERE claim_type IS NOT NULL
),
with_activity AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.end_date >= DATE '2025-11-20'
              AND p.product IS NOT NULL
        )
)
SELECT id, measure, metric_value
FROM (
    SELECT id, approved_amount, claimed_amount
    FROM with_activity
) src
UNPIVOT (
    metric_value FOR measure IN (approved_amount, claimed_amount)
) unp
ORDER BY id, measure
