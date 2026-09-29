WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
),
with_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.channel AS ticket_channel,
        t.csat AS ticket_csat,
        t.closed_at AS ticket_closed_at,
        su.team AS agent_team
    FROM prepared_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        ticket_channel,
        ticket_closed_at,
        agent_team,
        MAX(ticket_csat) OVER w_ticket_channel_agent_team AS ticket_channel_agent_team_max_ticket_csat,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type, ticket_channel ORDER BY ticket_closed_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY ticket_csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROUND(AVG(ticket_csat) OVER w_ticket_channel_agent_team, 2) AS ticket_channel_agent_team_avg_ticket_csat,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY event_type)) AS distinct_ticket_id_in_event_type,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY event_type)) AS distinct_actor_in_event_type,
        COUNT(*) OVER (PARTITION BY event_type) AS rows_in_event_type,
        MIN(ticket_csat) OVER w_event_type AS event_type_min_ticket_csat,
        LEAD(ticket_csat, 1, 0) OVER (PARTITION BY event_type, ticket_channel ORDER BY ticket_closed_at, id) AS next_ticket_csat,
        COUNT(*) OVER (PARTITION BY ticket_channel, agent_team) AS rows_in_ticket_channel_agent_team,
        LAG(event_at) OVER (ORDER BY ticket_closed_at ASC, id) AS prev_event_at,
        NTILE(5) OVER w_all_by_event_at AS event_at_quintile,
        SUM(ticket_csat) OVER w_ticket_channel_agent_team AS ticket_channel_agent_team_sum_ticket_csat
    FROM with_details_ticket
    WINDOW w_ticket_channel_agent_team AS (PARTITION BY ticket_channel, agent_team), w_event_type AS (PARTITION BY event_type), w_all_by_event_at AS (ORDER BY event_at ASC, id)
)
SELECT
    id,
    event_type,
    actor,
    ticket_channel,
    ticket_closed_at,
    ticket_channel_agent_team_max_ticket_csat,
    minutes_since_prev_event_at,
    last_known_event_at,
    ticket_channel_agent_team_avg_ticket_csat,
    distinct_ticket_id_in_event_type,
    distinct_actor_in_event_type,
    rows_in_event_type,
    event_type_min_ticket_csat,
    rows_in_ticket_channel_agent_team,
    prev_event_at,
    ticket_channel_agent_team_sum_ticket_csat,
    TO_DATE(ticket_closed_at) AS ticket_closed_at_date,
    ABS(event_at_quintile - 4) AS event_at_quintile_distance
FROM sequenced
WHERE event_type <> 'created'
ORDER BY id DESC, distinct_ticket_id_in_event_type NULLS LAST
