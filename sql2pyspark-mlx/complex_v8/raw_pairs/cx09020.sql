WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        claimed_amount - approved_amount AS claimed_amount_minus_approved_amount,
        claimed_amount + approved_amount AS claimed_amount_plus_approved_amount
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY adjuster_id) AS rows_in_adjuster_id,
        ROUND(AVG(approved_amount) OVER (ORDER BY claimed_amount_plus_approved_amount NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_4,
        MAX(claimed_amount) OVER (ORDER BY claimed_amount_plus_approved_amount NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS claimed_amount_max_7
    FROM cleaned
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(claim_type, 'ALL') AS claim_type,
    COALESCE(status, 'ALL') AS status,
    COUNT(*) AS row_count,
    SUM(CASE WHEN claim_type = 'collision' THEN claimed_amount_max_7 ELSE 0 END) AS claimed_amount_max_7_claim_type_collision
FROM windowed
GROUP BY GROUPING SETS ((claim_type, status), (claim_type), (status), ())
