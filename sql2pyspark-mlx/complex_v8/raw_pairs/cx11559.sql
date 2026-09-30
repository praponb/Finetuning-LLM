WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_parent_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.agent_id AS ticket_agent_id,
        t.priority AS ticket_priority,
        t.closed_at AS ticket_closed_at,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM valid s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        ticket_agent_id,
        ticket_closed_at,
        agent_hired_on,
        LEAD(agent_hired_on) OVER w_agent_team_by_event_at AS next_agent_hired_on,
        RANK() OVER w_agent_team_by_event_at_ties AS event_at_rank,
        LEAD(agent_hired_on) OVER (PARTITION BY agent_team, event_type ORDER BY agent_hired_on DESC, id) AS next_agent_hired_on_2,
        NTILE(3) OVER (PARTITION BY agent_team ORDER BY event_at ASC, id) AS event_at_tercile,
        NTH_VALUE(agent_hired_on, 2) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_agent_hired_on,
        LEAD(ticket_closed_at, 3) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on) ORDER BY agent_hired_on, id) AS next3_ticket_closed_at,
        RANK() OVER (PARTITION BY agent_team, event_type ORDER BY agent_hired_on DESC) AS agent_hired_on_rank,
        FIRST_VALUE(agent_hired_on) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on) ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_agent_hired_on,
        LAST_VALUE(ticket_closed_at) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_closed_at,
        LEAD(event_at) OVER (PARTITION BY agent_team, event_type ORDER BY agent_hired_on DESC, id) AS next_event_at,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on) ORDER BY agent_hired_on, id) AS agent_hired_on_dense_rank,
        LEAD(event_at, 3) OVER (ORDER BY event_at ASC, id) AS next3_event_at,
        NTILE(4) OVER (PARTITION BY agent_team ORDER BY event_at ASC, id) AS event_at_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_team ORDER BY event_at ASC), 3) AS event_at_percent_rank
    FROM with_parent_ticket
    WINDOW w_agent_team_by_event_at AS (PARTITION BY agent_team ORDER BY event_at ASC, id), w_agent_team_by_event_at_ties AS (PARTITION BY agent_team ORDER BY event_at ASC)
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        ticket_agent_id,
        ticket_closed_at,
        agent_hired_on,
        next_agent_hired_on,
        next_agent_hired_on_2,
        event_at_tercile,
        second_agent_hired_on,
        next3_ticket_closed_at,
        agent_hired_on_rank,
        first_agent_hired_on,
        last_ticket_closed_at,
        next_event_at,
        agent_hired_on_dense_rank,
        next3_event_at,
        event_at_quartile,
        event_at_percent_rank
    FROM ranked
    WHERE event_at_rank <= 5
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    ticket_closed_at,
    agent_hired_on,
    next_agent_hired_on,
    next_agent_hired_on_2,
    event_at_tercile,
    second_agent_hired_on,
    agent_hired_on_rank,
    last_ticket_closed_at,
    next_event_at,
    next3_event_at,
    event_at_quartile,
    LOWER(TRIM(actor)) AS actor_clean
FROM top_rows
WHERE YEAR(agent_hired_on) = 2024
ORDER BY last_ticket_closed_at DESC NULLS FIRST
