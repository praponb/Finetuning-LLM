WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label,
        CASE WHEN event_type IN ('escalated', 'reopened') THEN 'group_a' WHEN event_type = 'assigned' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND HOUR(event_at) BETWEEN 10 AND 16
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM recent_events s
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor IN ('system', 'customer')
        )
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        ticket_id,
        YEAR(event_at) AS event_at_year,
        event_type_group,
        ROUND(STDDEV(event_count), 2) AS stddev_event_count,
        COUNT(*) AS n_rows,
        SUM(CASE WHEN event_type <> 'replied' THEN 1 ELSE 0 END) AS n_event_type_replied
    FROM without_matches
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.ticket_id,
        s.event_at_year,
        s.event_type_group,
        s.n_rows,
        t.csat AS ticket_csat,
        t.opened_at AS ticket_opened_at,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM stats s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
    WHERE t.csat > 2
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.ticket_id,
        s.event_at_year,
        s.event_type_group,
        s.n_rows,
        s.ticket_csat,
        s.ticket_opened_at,
        s.agent_hired_on,
        s.agent_team,
        t.priority AS ticket_priority,
        t.channel AS ticket_channel,
        t.agent_id AS ticket_agent_id
    FROM joined s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
SELECT *
FROM (
    SELECT agent_team, ticket_priority, ticket_csat
    FROM with_parent
) src
PIVOT (
    AVG(ticket_csat)
    FOR ticket_priority IN ('P4', 'P1', 'P2')
)
