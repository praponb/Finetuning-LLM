WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        CASE WHEN event_type IN ('resolved', 'reopened') THEN 'group_a' WHEN event_type = 'created' THEN 'group_b' ELSE 'other' END AS event_type_group,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-02-06 11:00:00'
      AND event_type = 'assigned'
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor_or_unknown,
        event_type_group,
        event_type_slug,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label,
        SUBSTRING(TRIM(event_type), 1, 2) AS event_type_prefix,
        SUBSTRING(TRIM(event_type_group), 1, 2) AS event_type_group_prefix
    FROM valid_events
    WHERE event_type_slug IS NOT NULL
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor_or_unknown,
        s.event_type_group,
        s.event_at_label,
        s.event_type_prefix,
        s.event_type_group_prefix,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM scored s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(event_event_at) - UNIX_TIMESTAMP(event_at)) / 3600, 1) AS hours_event_at_to_event_event_at,
        LENGTH(TRIM(actor_or_unknown)) AS actor_or_unknown_len
    FROM detail_rows
),
per_group AS (
    -- Summarise per group
    SELECT
        event_type,
        ticket_id,
        event_id,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT event_type_group) AS distinct_event_type_groups,
        SUM(actor_or_unknown_len) AS total_actor_or_unknown_len,
        MIN(event_at) AS first_event_at,
        ROUND(100 * COUNT(CASE WHEN event_type IN ('created', 'reopened', 'assigned') THEN 1 END) / COUNT(*), 1) AS pct_event_type_in_created_reopen
    FROM annotated
    WHERE actor_or_unknown <> 'system'
    GROUP BY event_type, ticket_id, event_id
)
SELECT
    event_type,
    ticket_id,
    n_rows,
    total_actor_or_unknown_len,
    pct_event_type_in_created_reopen,
    SUBSTRING(TRIM(event_type), 1, 2) AS event_type_prefix
FROM per_group
WHERE event_id >= 90
ORDER BY event_type_prefix DESC
