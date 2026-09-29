WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type NOT IN ('reopened', 'escalated')
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.prev_event_at,
        s.new_session_flag,
        s.burst_id,
        t.csat AS ticket_csat,
        t.closed_at AS ticket_closed_at,
        t.channel AS ticket_channel
    FROM sessionized s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.csat <= 4
),
combined_details_ticket_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.prev_event_at,
        s.new_session_flag,
        s.burst_id,
        s.ticket_csat,
        s.ticket_closed_at,
        s.ticket_channel,
        t.channel AS ticket_channel_2,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM combined_details_ticket s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
SELECT
    id,
    ticket_id,
    event_type,
    prev_event_at,
    new_session_flag,
    burst_id,
    ticket_csat,
    ticket_closed_at,
    ticket_channel,
    ticket_channel_2,
    agent_team,
    agent_hired_on
FROM combined_details_ticket_2
WHERE new_session_flag < 1
