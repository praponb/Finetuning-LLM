WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        ROUND(claimed_amount * 1.07, 2) AS claimed_amount_adj,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_label
    FROM claims
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count
    FROM scoped s
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_counts s
    WHERE s.adjuster_id IN (
            SELECT c.adjuster_id
            FROM claims c
            WHERE c.approved_amount < 26237.1
        )
)
-- Final projection
SELECT DISTINCT reported_at, claimed_amount_adj, id
FROM filtered
ORDER BY id DESC, claimed_amount_adj DESC
