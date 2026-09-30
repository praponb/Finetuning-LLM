WITH filtered AS (
    -- Keep valid rows and derive helper columns
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
        ABS(approved_amount - 11215.8) AS approved_amount_distance
    FROM claims
    WHERE claimed_amount > 18364.1
),
joined AS (
    -- Join policies
    SELECT
        s.*,
        p.product AS policy_product,
        p.end_date AS policy_end_date,
        p.holder_id AS policy_holder_id,
        a.team AS adjuster_team,
        a.level AS adjuster_level
    FROM filtered s
    JOIN policies p ON p.id = s.policy_id AND p.start_date IS NOT NULL
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM joined
    WHERE adjuster_level < (SELECT MAX(adjuster_level) FROM joined)
),
deduped AS (
    -- Keep the latest row per policy_id
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
        approved_amount_distance,
        policy_product,
        policy_end_date,
        policy_holder_id,
        adjuster_team,
        adjuster_level
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY claim_date DESC, id DESC) AS row_num
        FROM qualified
    ) ranked
    WHERE row_num = 1
)
SELECT
    claim_type,
    policy_id,
    adjuster_level,
    ROUND(AVG(claimed_amount), 2) AS avg_claimed_amount,
    ARRAY_SORT(COLLECT_SET(adjuster_team)) AS adjuster_team_list,
    ROUND(SUM(claimed_amount) / NULLIF(SUM(approved_amount), 0), 3) AS claimed_amount_to_approved_amount_ratio
FROM deduped
GROUP BY CUBE(claim_type, policy_id, adjuster_level)
ORDER BY claim_type NULLS LAST, policy_id NULLS LAST, adjuster_level NULLS LAST
