WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        INITCAP(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE actor IN ('agent', 'customer')
      AND ticket_id IS NOT NULL
),
combined_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_type_clean,
        t.channel AS ticket_channel,
        t.agent_id AS ticket_agent_id,
        t.closed_at AS ticket_closed_at
    FROM scoped_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_parent AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.event_type_clean,
        s.ticket_channel,
        s.ticket_agent_id,
        t.channel AS ticket_channel_2,
        t.opened_at AS ticket_opened_at
    FROM combined_details_ticket s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND TO_DATE(t.opened_at) < DATE '2025-03-14'
),
totals AS (
    -- Group-level totals
    SELECT
        ticket_channel_2,
        ticket_id,
        DATE_TRUNC('MONTH', ticket_opened_at) AS ticket_opened_at_month,
        COUNT(DISTINCT ticket_channel_2) AS distinct_ticket_channel_2s,
        ROUND(100 * COUNT(CASE WHEN ticket_channel_2 NOT IN ('email', 'web') THEN 1 END) / COUNT(*), 1) AS pct_ticket_channel_2_not_in_emai,
        COUNT(DISTINCT ticket_channel) AS distinct_ticket_channels
    FROM with_parent
    WHERE event_type IS NOT NULL
    GROUP BY ticket_channel_2, ticket_id, DATE_TRUNC('MONTH', ticket_opened_at)
)
-- Final projection
SELECT
    ticket_channel_2,
    ticket_id,
    ticket_opened_at_month,
    CASE WHEN ticket_channel_2 IN ('chat', 'phone') THEN 'group_a' WHEN ticket_channel_2 = 'email' THEN 'group_b' ELSE 'other' END AS ticket_channel_2_group,
    LENGTH(TRIM(ticket_channel_2)) AS ticket_channel_2_len
FROM totals
ORDER BY ticket_channel_2_group DESC, ticket_id DESC NULLS FIRST
