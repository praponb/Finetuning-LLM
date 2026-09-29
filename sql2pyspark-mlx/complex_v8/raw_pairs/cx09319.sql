WITH base_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
        LEAST(requester_id, csat) AS least_requester_id_csat,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 3600, 1) AS hours_closed_at_to_opened_at
    FROM tickets
    WHERE channel IN ('web', 'chat', 'email')
      AND csat >= 2
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        tags,
        priority_or_unknown,
        least_requester_id_csat,
        hours_closed_at_to_opened_at,
        DATEDIFF(DATE '2025-12-31', closed_at) AS days_since_closed_at
    FROM base_tickets
    WHERE channel NOT IN ('email', 'phone')
),
scored_2 AS (
    -- Derive helper columns
    SELECT
        *,
        LEAST(days_since_closed_at, least_requester_id_csat) AS least_days_since_closed_at_least_requester_id_cs
    FROM scored
),
qualified AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM scored_2 s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.requester_id <= 14
              AND t.channel <> 'phone'
        )
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM qualified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_type IN ('assigned', 'resolved', 'replied')
              AND t.event_at >= TIMESTAMP '2025-01-31 13:00:00'
        )
)
SELECT
    id,
    agent_id,
    opened_at,
    channel,
    tags,
    priority_or_unknown,
    least_requester_id_csat,
    hours_closed_at_to_opened_at,
    days_since_closed_at,
    least_days_since_closed_at_least_requester_id_cs,
    CONCAT_WS('-', channel, priority_or_unknown) AS channel_priority_or_unknown_label,
    TO_DATE(opened_at) AS opened_at_date
FROM without_matches
WHERE days_since_closed_at > 280
ORDER BY channel_priority_or_unknown_label
