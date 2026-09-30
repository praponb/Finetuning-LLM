WITH recent_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE (ticket_id IS NOT NULL OR event_type IS NOT NULL)
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_at_label,
        s.actor_slug,
        s.event_type_slug,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM recent_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
latest AS (
    -- Keep the earliest row per event_actor
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        event_at_label,
        actor_slug,
        event_type_slug,
        event_id,
        event_actor,
        event_event_at,
        event_event_type
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_actor ORDER BY event_event_at, id, event_id) AS row_num
        FROM detail_rows_events
    ) ranked
    WHERE row_num = 1
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_at_label,
        s.actor_slug,
        s.event_type_slug,
        s.event_id,
        s.event_actor,
        s.event_event_at,
        s.event_event_type,
        t.closed_at AS ticket_closed_at
    FROM latest s
    JOIN tickets t ON t.id = s.ticket_id
),
per_group AS (
    -- Group-level totals
    SELECT
        event_event_type,
        SUM(CASE WHEN event_type IN ('reopened', 'escalated') THEN 1 ELSE 0 END) AS n_event_type_in_reopened_escal,
        ROUND(100 * COUNT(CASE WHEN actor NOT IN ('system', 'agent') THEN 1 END) / COUNT(*), 1) AS pct_actor_not_in_system_agent,
        ARRAY_SORT(COLLECT_SET(event_actor)) AS event_actor_list,
        COUNT(*) FILTER (WHERE event_event_type = 'assigned') AS n_event_event_type_assigned
    FROM with_details
    GROUP BY event_event_type
)
-- Final projection
SELECT
    event_event_type,
    n_event_type_in_reopened_escal,
    pct_actor_not_in_system_agent,
    event_actor_list,
    n_event_event_type_assigned,
    COALESCE(event_event_type, 'UNKNOWN') AS event_event_type_or_unknown,
    INITCAP(TRIM(event_event_type)) AS event_event_type_clean
FROM per_group
ORDER BY n_event_type_in_reopened_escal DESC NULLS FIRST
