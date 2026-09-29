WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix
    FROM ticket_events
    WHERE actor IN ('agent', 'system')
      AND MONTH(event_at) IN (1, 3)
),
latest AS (
    -- Keep the earliest row per actor, event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        days_since_event_at,
        event_type_actor_label,
        actor_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor, event_type ORDER BY event_at, id) AS row_num
        FROM scoped_events
    ) ranked
    WHERE row_num = 1
),
deduped AS (
    -- Keep the latest row per actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        days_since_event_at,
        event_type_actor_label,
        actor_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor ORDER BY days_since_event_at DESC, id DESC) AS recency_rank
        FROM latest
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    ticket_id,
    COALESCE(event_type, 'ALL') AS event_type,
    COALESCE(actor, 'ALL') AS actor,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list
FROM deduped
GROUP BY GROUPING SETS ((ticket_id, event_type, actor), (ticket_id), ())
