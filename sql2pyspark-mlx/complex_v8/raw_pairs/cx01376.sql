WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix,
        DATE_FORMAT(opened_at, 'MMM yyyy') AS opened_at_label
    FROM tickets
    WHERE agent_id >= 8
      AND csat < 4
),
with_parent_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.channel_prefix,
        s.opened_at_label,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM cleaned_tickets s
    JOIN support_agents su ON su.id = s.agent_id AND LOWER(su.agent_name) LIKE '% f%'
    WHERE su.hired_on < DATE '2025-02-27'
),
enriched AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.opened_at_label,
        s.agent_name,
        s.agent_hired_on,
        su.hired_on AS agent_hired_on_2,
        su.team AS agent_team,
        su.agent_name AS agent_agent_name
    FROM with_parent_agent s
    JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        opened_at_label,
        agent_name,
        agent_hired_on_2,
        agent_team,
        LAST_VALUE(agent_hired_on) IGNORE NULLS OVER (PARTITION BY channel ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_agent_hired_on,
        LEAD(agent_hired_on, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on_2) ORDER BY agent_hired_on_2 DESC, id) AS next2_agent_hired_on,
        NTH_VALUE(agent_hired_on, 2) OVER (PARTITION BY channel ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_agent_hired_on,
        NTILE(10) OVER (PARTITION BY channel ORDER BY agent_hired_on, id) AS agent_hired_on_decile,
        FIRST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY channel ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at
    FROM enriched
)
SELECT
    id,
    agent_id,
    opened_at_label,
    agent_hired_on_2,
    last_known_agent_hired_on,
    next2_agent_hired_on,
    second_agent_hired_on,
    agent_hired_on_decile,
    first_known_closed_at,
    ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(first_known_closed_at)) / 3600, 1) AS hours_first_known_closed_at_to_closed_at,
    DATEDIFF(second_agent_hired_on, agent_hired_on_2) AS days_agent_hired_on_2_to_second_agent_hired_on
FROM metrics
