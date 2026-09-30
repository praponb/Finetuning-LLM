WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CASE WHEN approved_amount >= 15477.7 THEN 'A' WHEN approved_amount >= 1273.7 THEN 'B' ELSE 'C' END AS approved_amount_tier,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket
    FROM claims
    WHERE approved_amount <= 17759.3
),
combined_details AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        s.reported_at,
        a.adjuster_name,
        a.level AS adjuster_level,
        a.team AS adjuster_team,
        p.product AS policy_product,
        p.premium AS policy_premium
    FROM valid s
    JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        approved_amount,
        status,
        reported_at,
        adjuster_team,
        policy_product,
        RANK() OVER (ORDER BY reported_at) AS reported_at_rank,
        approved_amount - LAG(approved_amount) OVER w_all_by_reported_at AS approved_amount_change
    FROM combined_details
    WINDOW w_all_by_reported_at AS (ORDER BY reported_at DESC, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        approved_amount,
        reported_at,
        policy_product,
        reported_at_rank,
        approved_amount_change,
        ROUND(approved_amount_change * 1.1, 2) AS approved_amount_change_adj,
        DATEDIFF(DATE '2025-12-31', reported_at) AS days_since_reported_at
    FROM with_trends
    WHERE reported_at < TIMESTAMP '2025-02-18 09:00:00'
)
-- Subtotals with GROUPING SETS
SELECT
    policy_product,
    policy_id,
    reported_at_rank,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT id) AS distinct_ids
FROM annotated
GROUP BY GROUPING SETS ((policy_product, policy_id, reported_at_rank), (policy_product), (policy_id))
ORDER BY policy_product NULLS LAST, policy_id NULLS LAST, reported_at_rank NULLS LAST
