WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-25 10:00:00'
      AND event_type IS NOT NULL
),
with_parent_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.channel AS ticket_channel
    FROM base s
    JOIN tickets t ON t.id = s.ticket_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_channel, actor ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROW_NUMBER() OVER w_event_type_by_event_at AS event_at_seq,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_channel, actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        RANK() OVER (PARTITION BY ticket_channel, actor ORDER BY event_at DESC) AS event_at_rank,
        LAG(event_at, 2) OVER (PARTITION BY ticket_channel, actor ORDER BY event_at DESC, id) AS prev2_event_at,
        NTILE(5) OVER (PARTITION BY ticket_id ORDER BY event_at DESC NULLS FIRST, id) AS event_at_quintile,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_channel ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_channel ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_3,
        NTILE(10) OVER (PARTITION BY event_type ORDER BY event_at DESC, id) AS event_at_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_channel, actor ORDER BY event_at DESC, id), 3) AS event_at_percent_rank,
        LEAD(event_at) OVER (PARTITION BY event_type ORDER BY event_at DESC, id) AS next_event_at,
        DENSE_RANK() OVER (PARTITION BY ticket_channel ORDER BY event_at) AS event_at_dense_rank
    FROM with_parent_ticket
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at DESC, id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    first_known_event_at,
    event_at_seq,
    event_at_quintile,
    first_event_at,
    first_known_event_at_3,
    event_at_decile,
    event_at_percent_rank,
    next_event_at,
    event_at_dense_rank,
    LOWER(REGEXP_REPLACE(TRIM(ticket_channel), ' ', '_')) AS ticket_channel_slug,
    CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref
FROM with_trends
