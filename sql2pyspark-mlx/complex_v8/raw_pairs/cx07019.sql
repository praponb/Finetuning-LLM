WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('created', 'reopened') THEN 'group_a' WHEN event_type = 'resolved' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-03-02 09:00:00'
      AND event_type IN ('replied', 'escalated')
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(event_type_group), ' ', '_')) AS event_type_group_slug,
        UPPER(TRIM(event_type_group)) AS event_type_group_clean
    FROM scoped_events
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_group,
        s.event_type_group_slug,
        t.requester_id AS ticket_requester_id,
        t.closed_at AS ticket_closed_at,
        t.channel AS ticket_channel
    FROM enriched s
    JOIN tickets t ON t.id = s.ticket_id
)
-- Subtotals with ROLLUP
SELECT
    ticket_channel,
    event_type,
    actor,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(actor)) AS actor_list
FROM with_parent
GROUP BY ROLLUP(ticket_channel, event_type, actor)
