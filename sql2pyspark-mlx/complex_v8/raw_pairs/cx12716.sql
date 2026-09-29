WITH matched AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(hours_since_open, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hours_since_open,
        MAX(hours_since_open) OVER (PARTITION BY agent_id) AS agent_id_max_hours_since_open,
        hours_since_open - LAG(hours_since_open) OVER (ORDER BY event_at, id) AS hours_since_open_change,
        ROUND(hours_since_open - AVG(hours_since_open) OVER w_agent_id, 2) AS hours_since_open_vs_agent_id_avg
    FROM matched
    WINDOW w_agent_id AS (PARTITION BY agent_id)
),
deduped AS (
    -- Keep the latest row per event_type, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        second_hours_since_open,
        agent_id_max_hours_since_open,
        hours_since_open_change,
        hours_since_open_vs_agent_id_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, ticket_id ORDER BY hours_since_open_vs_agent_id_avg DESC, id DESC) AS rn
        FROM metrics
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    priority,
    agent_id,
    hours_since_open,
    second_hours_since_open,
    hours_since_open_change,
    hours_since_open_vs_agent_id_avg,
    COALESCE(hours_since_open_change, 0.0) AS hours_since_open_change_filled
FROM deduped
