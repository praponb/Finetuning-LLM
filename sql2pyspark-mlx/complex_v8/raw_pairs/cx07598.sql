WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        DATE_FORMAT(reported_at, 'MMM yyyy') AS reported_at_label,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-02-24 12:00:00'
),
with_activity AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND a.team NOT IN ('Health', 'Motor')
              AND a.level > 3
        )
),
scored AS (
    -- Derive helper columns
    SELECT *, DATE_FORMAT(reported_at, 'yyyy-MM') AS reported_at_label_2
    FROM with_activity
    WHERE greatest_approved_amount_claimed_amount <= 32500.0
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT policy_id, status, claimed_amount
    FROM scored
) src
PIVOT (
    COUNT(claimed_amount)
    FOR status IN ('IN_REVIEW', 'REJECTED', 'PAID', 'APPROVED')
)
ORDER BY policy_id
