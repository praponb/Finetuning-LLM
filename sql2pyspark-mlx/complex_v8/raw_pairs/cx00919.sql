WITH filtered_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        DATEDIFF(DATE '2025-06-30', reported_at) AS days_since_reported_at,
        LENGTH(TRIM(status)) AS status_len,
        GREATEST(claimed_amount, approved_amount) AS greatest_claimed_amount_approved_amount
    FROM claims
    WHERE claimed_amount < 39666.5
      AND status IN ('OPEN', 'IN_REVIEW')
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM filtered_claims s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
)
-- Subtotals with GROUPING SETS
SELECT
    status,
    adjuster_id,
    SUM(status_len) AS total_status_len,
    MIN(greatest_claimed_amount_approved_amount) AS min_greatest_claimed_amount_approved_amount,
    ROUND(SUM(days_since_reported_at) / NULLIF(SUM(greatest_claimed_amount_approved_amount), 0), 3) AS days_since_reported_at_to_greatest_claimed_amoun
FROM qualified
GROUP BY GROUPING SETS ((status, adjuster_id), (status), (adjuster_id), ())
ORDER BY status NULLS LAST, adjuster_id NULLS LAST
