WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket
    FROM claims
    WHERE claimed_amount > 11235.6
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(claim_type, '#', CAST(adjuster_id AS STRING)) AS claim_type_ref,
        DATE_TRUNC('QUARTER', reported_at) AS reported_at_quarter
    FROM recent
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM derived s
    WHERE s.adjuster_id IN (
            SELECT c.adjuster_id
            FROM claims c
            WHERE YEAR(c.claim_date) = 2024
              AND MONTH(c.reported_at) IN (1, 3)
        )
)
SELECT *
FROM (
    SELECT adjuster_id, status, claim_type, approved_amount
    FROM screened
) src
PIVOT (
    COUNT(approved_amount)
    FOR claim_type IN ('collision', 'flood')
)
ORDER BY adjuster_id, status
