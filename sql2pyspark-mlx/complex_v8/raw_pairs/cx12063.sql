WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE LOWER(adjuster_name) LIKE '%wa%'
      AND level <= 3
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(adjuster_name), ' ', '_')) AS adjuster_name_slug,
        CASE WHEN team IN ('Motor', 'Travel') THEN 'group_a' WHEN team = 'Health' THEN 'group_b' ELSE 'other' END AS team_group,
        CASE WHEN team IN ('Travel', 'Property') THEN 'group_a' WHEN team = 'Motor' THEN 'group_b' ELSE 'other' END AS team_group_2
    FROM filtered
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.id
        )
      AND s.level <= 3
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.team,
        s.level,
        s.adjuster_name_slug,
        s.team_group,
        s.team_group_2,
        c.id AS claim_id,
        c.status AS claim_status,
        c.claim_type,
        c.policy_id AS claim_policy_id
    FROM with_activity s
    LEFT JOIN claims c ON c.adjuster_id = s.id AND c.claim_type = 'collision'
)
SELECT
    id,
    team,
    adjuster_name_slug,
    team_group,
    team_group_2,
    claim_id,
    claim_status,
    claim_type,
    ABS(level - 3) AS level_distance,
    ABS(level - 3) AS level_distance_2
FROM exploded_detail
