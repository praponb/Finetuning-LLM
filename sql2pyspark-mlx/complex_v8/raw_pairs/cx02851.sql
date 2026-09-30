WITH scoped_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref,
        approved_amount - claimed_amount AS approved_amount_minus_claimed_amount
    FROM claims
    WHERE claimed_amount >= 4868.7
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        ROUND(approved_amount * 0.25, 2) AS approved_amount_adj
    FROM scoped_claims
),
status_stats AS (
    -- Max approved_amount_adj per status
    SELECT status, MAX(approved_amount_adj) AS max_approved_amount_adj
    FROM tagged
    GROUP BY status
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_approved_amount_adj,
        ROUND(s.approved_amount_adj - g.max_approved_amount_adj, 2) AS approved_amount_adj_minus_max
    FROM tagged s
    JOIN status_stats g ON g.status = s.status
    WHERE s.approved_amount_adj = g.max_approved_amount_adj
)
-- Subtotals with CUBE
SELECT
    status,
    adjuster_id,
    policy_id,
    COUNT(DISTINCT status_or_unknown) AS distinct_status_or_unknowns,
    ROUND(100 * COUNT(CASE WHEN status = 'OPEN' THEN 1 END) / COUNT(*), 1) AS pct_status_open
FROM vs_group
GROUP BY CUBE(status, adjuster_id, policy_id)
