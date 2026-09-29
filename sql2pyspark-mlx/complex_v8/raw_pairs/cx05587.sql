WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix
    FROM ticket_events
    WHERE actor IS NOT NULL
      AND event_at >= TIMESTAMP '2025-02-06 11:00:00'
),
deduped AS (
    -- Keep the latest row per ticket_id, event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id DESC) AS recency_rank
        FROM recent_events
    ) ranked
    WHERE recency_rank = 1
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM deduped s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
      AND s.event_type = 'created'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_prefix,
        LAG(event_at, 3) OVER (PARTITION BY MONTH(event_at) ORDER BY event_at DESC, id) AS prev3_event_at,
        NTILE(10) OVER (PARTITION BY MONTH(event_at) ORDER BY event_at DESC, id) AS event_at_decile,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY MONTH(event_at) ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY MONTH(event_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROW_NUMBER() OVER (PARTITION BY MONTH(event_at) ORDER BY event_at DESC, id) AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(event_at) ORDER BY event_at DESC), 3) AS event_at_cume_dist
    FROM with_activity
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_prefix,
        event_at_decile,
        minutes_since_prev_event_at,
        last_known_event_at,
        rn,
        event_at_cume_dist,
        event_at_decile - rn AS event_at_decile_minus_rn,
        DATE_TRUNC('YEAR', last_known_event_at) AS last_known_event_at_year,
        event_at + INTERVAL 15 MINUTES AS event_at_plus_15m
    FROM sequenced
    WHERE event_type = 'created'
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor_prefix,
        s.minutes_since_prev_event_at,
        s.event_at_decile_minus_rn,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM derived s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    actor_prefix,
    minutes_since_prev_event_at,
    event_at_decile_minus_rn,
    event_id,
    event_event_type,
    event_event_at,
    event_actor,
    CONCAT_WS(' / ', event_actor, event_event_type) AS event_actor_event_event_type_label
FROM with_children_events
ORDER BY event_event_at DESC, event_event_type
