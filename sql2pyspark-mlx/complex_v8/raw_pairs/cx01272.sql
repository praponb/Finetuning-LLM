WITH base_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled_2
    FROM claims
    WHERE claimed_amount >= 3703.0
),
with_parent_adjuster AS (
    -- Join adjusters
    SELECT
        s.*,
        a.adjuster_name,
        a.level AS adjuster_level,
        p.start_date AS policy_start_date,
        p.premium AS policy_premium
    FROM base_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
    WHERE a.level < 3
)
SELECT
    adjuster_id,
    adjuster_level,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT policy_id) AS distinct_policy_ids
FROM with_parent_adjuster
GROUP BY ROLLUP(adjuster_id, adjuster_level)
