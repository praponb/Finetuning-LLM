WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 30 MINUTES AS event_at_plus_30m,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE actor = 'system'
      AND event_type IN ('replied', 'escalated', 'created')
),
latest_per_key AS (
    -- Keep the earliest row per event_type, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_plus_30m,
        event_type_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at_plus_30m, id) AS row_num
        FROM prepared_events
    ) ranked
    WHERE row_num = 1
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER w_ticket_id_by_event_at_ties AS event_at_rank,
        ROW_NUMBER() OVER w_ticket_id_by_event_at AS event_at_seq,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at
    FROM latest_per_key
    WINDOW w_ticket_id_by_event_at_ties AS (PARTITION BY ticket_id ORDER BY event_at DESC), w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at DESC, id)
)
SELECT
    id,
    event_at,
    event_type,
    event_at_plus_30m,
    event_at_seq,
    second_event_at
FROM ranked
