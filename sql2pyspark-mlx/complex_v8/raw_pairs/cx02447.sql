WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('replied', 'escalated') THEN 'group_a' WHEN event_type = 'assigned' THEN 'group_b' ELSE 'other' END AS event_type_group,
        DATE_TRUNC('DAY', event_at) AS event_at_day,
        CASE WHEN actor IN ('customer', 'agent') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE event_type = 'resolved'
),
enriched AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_type_group,
        s.event_at_day,
        t.opened_at AS ticket_opened_at
    FROM prepared_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.opened_at >= TIMESTAMP '2025-02-06 11:00:00'
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(': ', event_type, event_type_group) AS event_type_event_type_group_label,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        CONCAT(event_type_group, '#', CAST(id AS STRING)) AS event_type_group_ref
    FROM enriched
),
labeled_2 AS (
    -- Add calculated columns
    SELECT
        id,
        event_type,
        actor,
        event_type_group,
        event_at_day,
        ticket_opened_at,
        event_type_event_type_group_label,
        actor_slug,
        MONTH(event_at_day) AS event_at_day_month
    FROM labeled
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        event_type,
        actor,
        event_at_day,
        event_type_event_type_group_label,
        actor_slug,
        COALESCE(event_type_group, 'UNKNOWN') AS event_type_group_or_unknown
    FROM labeled_2
),
enriched_2 AS (
    -- Add calculated columns
    SELECT
        id,
        event_type,
        actor,
        event_at_day,
        actor_slug,
        event_type_group_or_unknown,
        QUARTER(event_at_day) AS event_at_day_quarter,
        QUARTER(event_at_day) AS event_at_day_quarter_2
    FROM annotated
)
-- Final projection
SELECT
    id,
    actor,
    event_at_day,
    actor_slug,
    event_at_day_quarter,
    event_at_day_quarter_2,
    CONCAT_WS('|', actor_slug, actor) AS actor_slug_actor_label
FROM enriched_2
