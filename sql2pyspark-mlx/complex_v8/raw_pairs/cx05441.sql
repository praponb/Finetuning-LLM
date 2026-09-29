WITH joined_ranges AS (
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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        hours_since_open - LAG(hours_since_open) OVER (PARTITION BY priority, agent_id ORDER BY hours_since_open DESC, id) AS hours_since_open_change,
        ROW_NUMBER() OVER (ORDER BY hours_since_open DESC, id) AS rn,
        ROUND(hours_since_open / NULLIF(SUM(hours_since_open) OVER (PARTITION BY priority, event_type), 0), 4) AS hours_since_open_share_of_priority_event_type
    FROM joined_ranges
),
with_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        s.hours_since_open_change,
        s.rn,
        s.hours_since_open_share_of_priority_event_type,
        su.agent_name,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team,
        t.tags AS ticket_tags,
        t.closed_at AS ticket_closed_at
    FROM windowed s
    JOIN support_agents su ON su.id = s.agent_id AND UPPER(TRIM(su.agent_name)) LIKE 'F%'
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
unique_rows AS (
    -- Keep the latest row per agent_id, priority
    SELECT
        id,
        ticket_id,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        hours_since_open_change,
        rn,
        hours_since_open_share_of_priority_event_type,
        agent_name,
        agent_hired_on,
        agent_team,
        ticket_tags,
        ticket_closed_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, priority ORDER BY hours_since_open DESC, id DESC) AS recency_rank
        FROM with_details_agent
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    event_type,
    priority,
    agent_id,
    hours_since_open,
    rn,
    hours_since_open_share_of_priority_event_type,
    agent_name,
    ticket_tags,
    ticket_closed_at
FROM unique_rows
WHERE LENGTH(TRIM(agent_name)) >= 7
ORDER BY ticket_closed_at DESC
