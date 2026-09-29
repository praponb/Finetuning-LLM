WITH recent_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('reopened', 'created') THEN 'group_a' WHEN event_type = 'replied' THEN 'group_b' ELSE 'other' END AS event_type_group,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE (actor IN ('system', 'agent') OR ticket_id <= 14)
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type_group,
        s.actor_or_unknown,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM recent_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        actor_or_unknown,
        event_id,
        event_event_type,
        LEAD(event_at, 3) OVER w_event_id_ticket_id_by_event AS next3_event_at,
        LAST_VALUE(event_event_at) IGNORE NULLS OVER (PARTITION BY event_id, actor_or_unknown ORDER BY event_event_at, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_event_at,
        LAG(event_event_at) OVER (PARTITION BY event_event_type, event_id ORDER BY event_event_at DESC, id, event_id) AS prev_event_event_at,
        NTILE(4) OVER (PARTITION BY event_id, actor_or_unknown ORDER BY event_event_at, id, event_id) AS event_event_at_quartile
    FROM detail_rows
    WINDOW w_event_id_ticket_id_by_event AS (PARTITION BY event_id, ticket_id ORDER BY event_at DESC, id, event_id)
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at
    FROM windowed s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    event_id,
    event_event_type,
    next3_event_at,
    last_known_event_event_at,
    event_event_at_quartile,
    LOWER(TRIM(event_event_type)) AS event_event_type_clean
FROM profiled
ORDER BY event_event_type_clean
