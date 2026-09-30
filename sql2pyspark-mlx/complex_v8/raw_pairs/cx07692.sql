WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount,
        claimed_amount + approved_amount AS claimed_amount_plus_approved_amount
    FROM claims
    WHERE (claim_date IS NOT NULL OR status IN ('PAID', 'OPEN'))
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        least_approved_amount_claimed_amount,
        CASE WHEN claim_type IN ('fire', 'delay') THEN 'group_a' WHEN claim_type = 'medical' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM valid
    WHERE claim_date < DATE '2025-05-26'
),
grouped AS (
    -- Summarise per group
    SELECT
        policy_id,
        COUNT(*) FILTER (WHERE claim_type <> 'theft') AS n_claim_type_theft,
        COUNT(*) AS row_count,
        DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days,
        ROUND(SUM(approved_amount) / NULLIF(SUM(least_approved_amount_claimed_amount), 0), 3) AS approved_amount_to_least_approved_amount_claimed
    FROM scored
    GROUP BY policy_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT policy_id, metric, metric_value
FROM (
    SELECT policy_id, n_claim_type_theft, row_count
    FROM grouped
) src
UNPIVOT (
    metric_value FOR metric IN (n_claim_type_theft AS n_claim_type_theft_v, row_count AS row_count_v)
) unp
