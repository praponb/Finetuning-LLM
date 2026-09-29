WITH scoped_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount
    FROM claims
    WHERE claim_date < DATE '2025-09-14'
      AND policy_id <= 25
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_id,
        claim_type,
        approved_amount,
        approved_amount_per_claimed_amount,
        COUNT(*) OVER (PARTITION BY claim_type ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTH_VALUE(claimed_amount, 2) OVER (PARTITION BY claim_type ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        ROUND(AVG(approved_amount) OVER (PARTITION BY TO_DATE(reported_at)), 2) AS reported_at_to_date_avg_approved_amount,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY claim_type ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at
    FROM scoped_claims
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT second_reported_at, adjuster_id, claim_type, running_count
    FROM ranked
) src
PIVOT (
    COUNT(running_count)
    FOR claim_type IN ('fire', 'collision')
)
