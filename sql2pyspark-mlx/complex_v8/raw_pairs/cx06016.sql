WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE event_type = 'resolved'
),
latest AS (
    -- Keep the latest row per actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_event_type_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor ORDER BY event_at DESC, id DESC) AS recency_rank
        FROM filtered
    ) ranked
    WHERE recency_rank = 1
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        event_at,
        actor,
        actor_event_type_label,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY event_at DESC, id), 3) AS event_at_percent_rank,
        LEAD(event_at) OVER w_event_type_by_event_at AS next_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        LEAD(event_at, 2) OVER w_ticket_id_by_event_at AS next2_event_at
    FROM latest
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at DESC, id), w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at DESC, id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT next_event_at, actor, minutes_since_prev_event_at
    FROM sequenced
) src
PIVOT (
    SUM(minutes_since_prev_event_at) AS total, COUNT(minutes_since_prev_event_at) AS n
    FOR actor IN ('customer', 'agent')
)
ORDER BY next_event_at
