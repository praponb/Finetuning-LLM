WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        priority,
        channel,
        csat,
        tags,
        COALESCE(csat, 0) AS csat_filled
    FROM tickets
    WHERE csat > 3
      AND requester_id >= 4
),
exploded AS (
    -- One row per tag
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.channel,
        s.csat,
        s.csat_filled,
        t.tag_pos,
        t.tag
    FROM base s
    LATERAL VIEW OUTER POSEXPLODE(s.tags) t AS tag_pos, tag
),
with_parent_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.channel,
        s.csat,
        s.csat_filled,
        s.tag_pos,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team,
        su.agent_name
    FROM exploded s
    JOIN support_agents su ON su.id = s.agent_id
),
joined AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.csat,
        s.csat_filled,
        s.tag_pos,
        s.agent_hired_on,
        s.agent_team,
        s.agent_name,
        su.agent_name AS agent_agent_name
    FROM with_parent_agent s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE LENGTH(TRIM(su.agent_name)) >= 4
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.event_type IN ('created', 'replied')) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_event_type_count,
        (SELECT COUNT(CASE WHEN t.actor = 'agent' THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_actor_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at
    FROM joined s
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY tag_pos ORDER BY last_event_at ASC, id, tag_pos) AS last_event_at_tercile,
        RANK() OVER w_tag_pos_by_last_event_at_tie AS last_event_at_rank,
        ROUND(event_event_type_count - AVG(event_event_type_count) OVER (PARTITION BY csat, csat_filled), 2) AS event_event_type_count_vs_csat_csat_filled_avg,
        COUNT(*) OVER (PARTITION BY csat, csat_filled) AS rows_in_csat_csat_filled
    FROM profiled
    WINDOW w_tag_pos_by_last_event_at_tie AS (PARTITION BY tag_pos ORDER BY last_event_at ASC)
)
SELECT
    id,
    agent_id,
    csat,
    csat_filled,
    tag_pos,
    agent_name,
    agent_agent_name,
    event_event_type_count,
    event_actor_count,
    last_event_at,
    last_event_at_tercile,
    event_event_type_count_vs_csat_csat_filled_avg,
    rows_in_csat_csat_filled
FROM with_trends
ORDER BY agent_agent_name
