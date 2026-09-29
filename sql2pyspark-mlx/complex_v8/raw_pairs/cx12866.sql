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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
exploded_detail AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        t.id AS ticket_id_2,
        t.tags AS ticket_tags,
        t.requester_id AS ticket_requester_id,
        t.priority AS ticket_priority,
        t.channel AS ticket_channel
    FROM joined_ranges s
    LEFT JOIN tickets t ON t.agent_id = s.agent_id AND t.channel = 'phone'
),
latest_per_key AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        ticket_id_2,
        ticket_tags,
        ticket_requester_id,
        ticket_priority,
        ticket_channel
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY hours_since_open DESC, id DESC, ticket_id_2 DESC) AS rn
        FROM exploded_detail
    ) ranked
    WHERE rn = 1
),
ticket_id_stats AS (
    -- Avg hours_since_open per ticket_id
    SELECT ticket_id, AVG(hours_since_open) AS avg_hours_since_open
    FROM latest_per_key
    GROUP BY ticket_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_hours_since_open,
        ROUND(s.hours_since_open - g.avg_hours_since_open, 2) AS hours_since_open_minus_avg
    FROM latest_per_key s
    JOIN ticket_id_stats g ON g.ticket_id = s.ticket_id
    WHERE s.hours_since_open > g.avg_hours_since_open
)
SELECT
    ticket_id,
    COALESCE(ticket_channel, 'ALL') AS ticket_channel,
    ARRAY_SORT(COLLECT_SET(ticket_priority)) AS ticket_priority_list
FROM vs_group
GROUP BY ROLLUP(ticket_id, ticket_channel)
ORDER BY ticket_id NULLS LAST, ticket_channel NULLS LAST
