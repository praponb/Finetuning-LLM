WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        tags,
        GREATEST(csat, requester_id) AS greatest_csat_requester_id
    FROM tickets
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT s.*, su.agent_name, su.hired_on AS agent_hired_on
    FROM filtered s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
flattened AS (
    -- One row per tag
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.greatest_csat_requester_id,
        s.agent_name,
        s.agent_hired_on,
        t.tag
    FROM with_parent_agent s
    LATERAL VIEW EXPLODE(s.tags) t AS tag
),
enriched AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.agent_hired_on,
        s.tag,
        su.team AS agent_team,
        su.agent_name AS agent_agent_name
    FROM flattened s
    JOIN support_agents su ON su.id = s.agent_id AND UPPER(TRIM(su.agent_name)) LIKE 'J%'
    WHERE su.team IS NOT NULL
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(5) OVER w_agent_id_priority_by_agent_h AS agent_hired_on_quintile,
        ROW_NUMBER() OVER (PARTITION BY agent_id, priority ORDER BY agent_hired_on DESC, id, tag) AS agent_hired_on_seq,
        NTILE(4) OVER w_agent_id_priority_by_agent_h AS agent_hired_on_quartile
    FROM enriched
    WINDOW w_agent_id_priority_by_agent_h AS (PARTITION BY agent_id, priority ORDER BY agent_hired_on DESC, id, tag)
),
with_parent_agent_2 AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.agent_hired_on,
        s.tag,
        s.agent_agent_name,
        s.agent_hired_on_quartile,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on_2,
        su.team AS agent_team_2
    FROM with_windows s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    agent_id,
    priority,
    agent_hired_on,
    tag,
    agent_hired_on_quartile,
    agent_tier,
    agent_hired_on_2,
    agent_team_2,
    LENGTH(TRIM(agent_team_2)) AS agent_team_2_len,
    QUARTER(agent_hired_on) AS agent_hired_on_quarter
FROM with_parent_agent_2
WHERE agent_id IS NOT NULL
ORDER BY tag, agent_hired_on, id
LIMIT 20
