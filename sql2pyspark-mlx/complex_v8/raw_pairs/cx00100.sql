WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        CASE WHEN claim_type IN ('flood', 'theft') THEN 'group_a' WHEN claim_type = 'collision' THEN 'group_b' ELSE 'other' END AS claim_type_group,
        ROUND(claimed_amount * 1.07, 2) AS claimed_amount_adj,
        ROUND(approved_amount * 0.25, 2) AS approved_amount_adj
    FROM claims
    WHERE claim_date < DATE '2025-04-17'
      AND claimed_amount >= 17585.2
),
combined_details_adjuster AS (
    -- Join adjusters
    SELECT
        s.*,
        a.adjuster_name,
        a.level AS adjuster_level,
        a.team AS adjuster_team,
        p.status AS policy_status,
        p.start_date AS policy_start_date
    FROM recent s
    JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
latest AS (
    -- Keep the earliest row per adjuster_team
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        claim_type_group,
        claimed_amount_adj,
        approved_amount_adj,
        adjuster_name,
        adjuster_level,
        adjuster_team,
        policy_status,
        policy_start_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_team ORDER BY approved_amount_adj, id) AS rn
        FROM combined_details_adjuster
    ) ranked
    WHERE rn = 1
),
latest_2 AS (
    -- Keep the latest row per status, claim_type_group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        claim_type_group,
        claimed_amount_adj,
        approved_amount_adj,
        adjuster_name,
        adjuster_level,
        adjuster_team,
        policy_status,
        policy_start_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status, claim_type_group ORDER BY approved_amount DESC, id DESC) AS rn
        FROM latest
    ) ranked
    WHERE rn = 1
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(adjuster_team, 'ALL') AS adjuster_team,
    COALESCE(claim_type, 'ALL') AS claim_type,
    adjuster_id,
    ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list
FROM latest_2
GROUP BY ROLLUP(adjuster_team, claim_type, adjuster_id)
