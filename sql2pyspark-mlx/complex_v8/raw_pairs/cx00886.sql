WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_at < TIMESTAMP '2025-02-24 11:00:00'
),
without_matches AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM base_events s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.csat < 5
              AND SIZE(t.tags) >= 2
        )
),
deduped AS (
    -- Keep the earliest row per ticket_id, event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_actor_label,
        actor_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id) AS row_num
        FROM without_matches
    ) ranked
    WHERE row_num = 1
)
SELECT
    ticket_id,
    COALESCE(event_type, 'ALL') AS event_type,
    COALESCE(actor, 'ALL') AS actor,
    ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list
FROM deduped
GROUP BY ROLLUP(ticket_id, event_type, actor)
ORDER BY ticket_id NULLS LAST, event_type NULLS LAST, actor NULLS LAST
