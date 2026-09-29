WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount
    FROM claims
),
policy_id_stats AS (
    -- Avg approved_amount per policy_id
    SELECT policy_id, AVG(approved_amount) AS avg_approved_amount
    FROM recent
    GROUP BY policy_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_approved_amount,
        ROUND(s.approved_amount - g.avg_approved_amount, 2) AS approved_amount_minus_avg
    FROM recent s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.approved_amount > g.avg_approved_amount
),
qualified AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM vs_group s
    WHERE EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND a.level <= 3
              AND a.team IN ('Property', 'Health')
        )
)
-- Subtotals with GROUPING SETS
SELECT
    policy_id,
    adjuster_id,
    COALESCE(claim_type, 'ALL') AS claim_type,
    ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
    ROUND(SUM(approved_amount), 2) AS total_approved_amount
FROM qualified
GROUP BY GROUPING SETS ((policy_id, adjuster_id, claim_type), (policy_id))
ORDER BY policy_id NULLS LAST, adjuster_id NULLS LAST, claim_type NULLS LAST
