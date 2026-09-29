WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
    WHERE event_type IN ('assigned', 'reopened', 'replied')
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        t.closed_at AS ticket_closed_at,
        t.agent_id AS ticket_agent_id,
        t.channel AS ticket_channel,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM base_events s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
deduped AS (
    -- Keep the earliest row per event_type
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        ticket_closed_at,
        ticket_agent_id,
        ticket_channel,
        agent_hired_on,
        agent_team
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY ticket_closed_at, id) AS rn
        FROM joined
    ) ranked
    WHERE rn = 1
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY event_type ORDER BY ticket_closed_at ASC) AS ticket_closed_at_rank,
        ROUND(CUME_DIST() OVER (ORDER BY agent_hired_on DESC), 3) AS agent_hired_on_cume_dist,
        LAST_VALUE(agent_hired_on) OVER (PARTITION BY event_type ORDER BY ticket_closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_agent_hired_on,
        RANK() OVER (PARTITION BY event_type ORDER BY ticket_closed_at ASC, id) AS ticket_closed_at_rank_2
    FROM deduped
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, ticket_closed_at_rank, ticket_closed_at_rank_2
    FROM metrics
) src
UNPIVOT (
    metric_value FOR measure IN (ticket_closed_at_rank, ticket_closed_at_rank_2)
) unp
ORDER BY id, measure
