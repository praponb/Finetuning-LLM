WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor IN ('agent', 'system')
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.priority AS ticket_priority,
        t.tags AS ticket_tags,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        ticket_priority,
        ticket_tags,
        agent_hired_on,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor, agent_team ORDER BY agent_hired_on DESC, id), 3) AS agent_hired_on_percent_rank,
        ROUND(PERCENT_RANK() OVER w_agent_team_by_event_at_ties, 3) AS event_at_percent_rank,
        RANK() OVER (PARTITION BY actor, agent_team ORDER BY agent_hired_on DESC, id) AS agent_hired_on_rank,
        FIRST_VALUE(event_at) OVER (PARTITION BY event_type ORDER BY agent_hired_on NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        NTILE(10) OVER (PARTITION BY event_type ORDER BY agent_hired_on NULLS LAST, id) AS agent_hired_on_decile,
        RANK() OVER (PARTITION BY actor, agent_team ORDER BY event_at) AS event_at_rank,
        LEAD(event_at, 3) OVER (PARTITION BY actor, agent_team ORDER BY event_at, id) AS next3_event_at,
        LEAD(agent_hired_on) OVER w_agent_team_by_event_at AS next_agent_hired_on,
        NTILE(3) OVER w_agent_team_by_event_at AS event_at_tercile,
        DENSE_RANK() OVER (PARTITION BY YEAR(agent_hired_on) ORDER BY event_at, id) AS event_at_dense_rank,
        LAG(agent_hired_on) OVER (PARTITION BY YEAR(agent_hired_on) ORDER BY event_at, id) AS prev_agent_hired_on,
        LEAD(event_at) OVER w_actor_agent_team_by_event_at_2 AS next_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(agent_hired_on) ORDER BY event_at), 3) AS event_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY agent_team ORDER BY event_at NULLS LAST, id))) / 60, 1) AS minutes_since_prev_event_at,
        FIRST_VALUE(agent_hired_on) IGNORE NULLS OVER (PARTITION BY agent_team ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_agent_hired_on
    FROM enriched_ticket
    WINDOW w_agent_team_by_event_at_ties AS (PARTITION BY agent_team ORDER BY event_at NULLS LAST), w_agent_team_by_event_at AS (PARTITION BY agent_team ORDER BY event_at NULLS LAST, id), w_actor_agent_team_by_event_at_2 AS (PARTITION BY actor, agent_team ORDER BY event_at, id)
)
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    ticket_priority,
    ticket_tags,
    agent_hired_on_percent_rank,
    event_at_percent_rank,
    first_event_at,
    agent_hired_on_decile,
    event_at_rank,
    next_agent_hired_on,
    event_at_tercile,
    event_at_dense_rank,
    prev_agent_hired_on,
    next_event_at,
    event_at_cume_dist,
    minutes_since_prev_event_at,
    first_known_agent_hired_on,
    SIZE(ticket_tags) AS ticket_tags_count
FROM metrics
WHERE first_known_agent_hired_on >= DATE '2024-07-09'
ORDER BY ticket_tags_count NULLS LAST, id
LIMIT 10
