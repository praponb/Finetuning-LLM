WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        CONCAT_WS('-', tier, agent_name) AS tier_agent_name_label,
        CASE WHEN tier IN ('T1', 'T2') THEN 'group_a' WHEN tier = 'T3' THEN 'group_b' ELSE 'other' END AS tier_group,
        COALESCE(tier, 'UNKNOWN') AS tier_or_unknown
    FROM support_agents
    WHERE LOWER(agent_name) LIKE '%no%'
      AND tier = 'T2'
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        agent_name,
        tier,
        tier_or_unknown,
        DATE_ADD(hired_on, 365) AS hired_on_plus_365d,
        YEAR(hired_on) AS hired_on_year,
        LOWER(REGEXP_REPLACE(TRIM(team), ' ', '_')) AS team_slug
    FROM filtered
),
unique_rows AS (
    -- Keep the latest row per tier_or_unknown, tier
    SELECT
        id,
        agent_name,
        tier,
        tier_or_unknown,
        hired_on_plus_365d,
        hired_on_year,
        team_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tier_or_unknown, tier ORDER BY hired_on_plus_365d DESC, id DESC) AS row_num
        FROM labeled
    ) ranked
    WHERE row_num = 1
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        agent_name,
        tier,
        hired_on_plus_365d,
        hired_on_year,
        team_slug,
        COALESCE(tier_or_unknown, 'UNKNOWN') AS tier_or_unknown_or_unknown
    FROM unique_rows
    WHERE hired_on_plus_365d IS NOT NULL
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MAX(t.opened_at) FROM tickets t WHERE t.agent_id = s.id) AS last_ticket_opened_at,
        (SELECT MIN(t.opened_at) FROM tickets t WHERE t.agent_id = s.id) AS first_ticket_opened_at,
        (SELECT MIN(t.opened_at) FROM tickets t WHERE t.agent_id = s.id) AS first_ticket_opened_at_2
    FROM tagged s
)
-- Final projection
SELECT
    id,
    agent_name,
    team_slug,
    last_ticket_opened_at,
    CASE WHEN tier IN ('T1', 'T2') THEN 'group_a' WHEN tier = 'T3' THEN 'group_b' ELSE 'other' END AS tier_group
FROM profiled
WHERE agent_name IS NOT NULL
ORDER BY id DESC, last_ticket_opened_at DESC
LIMIT 10
