WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug,
        ROUND(claimed_amount * 0.25, 2) AS claimed_amount_adj
    FROM claims
    WHERE claim_date < DATE '2025-09-14'
      AND adjuster_id <= 3
),
with_parent AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status_ref,
        s.claimed_amount_adj,
        p.product AS policy_product
    FROM filtered s
    LEFT JOIN policies p ON p.id = s.policy_id AND p.premium BETWEEN 986.9 AND 4678.8
),
with_parent_2 AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.claimed_amount_adj,
        p.end_date AS policy_end_date,
        p.start_date AS policy_start_date,
        a.team AS adjuster_team
    FROM with_parent s
    LEFT JOIN policies p ON p.id = s.policy_id AND p.end_date IS NOT NULL
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
filtered_2 AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_parent_2 s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
        )
      AND s.adjuster_id IS NOT NULL
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(adjuster_team, 'ALL') AS adjuster_team,
    policy_id,
    adjuster_id,
    ROUND(100 * COUNT(CASE WHEN claim_type = 'delay' THEN 1 END) / COUNT(*), 1) AS pct_claim_type_delay,
    ROUND(STDDEV(claimed_amount_adj), 2) AS stddev_claimed_amount_adj,
    COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids
FROM filtered_2
GROUP BY GROUPING SETS ((adjuster_team, policy_id, adjuster_id), (adjuster_team), ())
ORDER BY adjuster_team NULLS LAST, policy_id NULLS LAST, adjuster_id NULLS LAST
