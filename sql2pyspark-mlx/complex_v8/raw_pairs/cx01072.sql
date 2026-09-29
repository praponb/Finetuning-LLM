WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('WEEK', event_at) AS event_at_week,
        SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix
    FROM ticket_events
    WHERE actor IN ('agent', 'customer')
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        COUNT(*) AS event_count,
        MAX(event_at) AS last_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        COALESCE(t.event_count, 0) AS event_count,
        t.last_event_at_2
    FROM valid s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
deduped AS (
    -- Keep the earliest row per ticket_id, event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_week,
        actor_prefix,
        last_event_at,
        event_count,
        last_event_at_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type ORDER BY last_event_at, id) AS rn
        FROM summarized_rows
    ) ranked
    WHERE rn = 1
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.last_event_at,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM deduped s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        event_id,
        event_event_type,
        FIRST_VALUE(last_event_at) IGNORE NULLS OVER (ORDER BY event_event_at ASC, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_event_at,
        NTILE(3) OVER (ORDER BY event_event_at ASC, id, event_id) AS event_event_at_tercile,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_event_at ASC, id, event_id), 3) AS event_event_at_percent_rank,
        LAST_VALUE(last_event_at) IGNORE NULLS OVER (ORDER BY event_event_at ASC, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_event_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_event_at ASC), 3) AS event_event_at_percent_rank_2,
        FIRST_VALUE(event_event_at) OVER (ORDER BY event_event_at ASC, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_event_at
    FROM line_level
),
with_details_event AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.event_id,
        s.first_known_last_event_at,
        s.event_event_at_tercile,
        s.event_event_at_percent_rank,
        s.event_event_at_percent_rank_2,
        s.first_event_event_at,
        t.ticket_id AS event_ticket_id,
        t.event_at,
        t.event_type
    FROM with_windows s
    LEFT JOIN ticket_events t ON t.id = s.event_id AND t.event_type <> 'assigned'
)
SELECT
    id,
    event_id,
    first_known_last_event_at,
    event_event_at_tercile,
    event_event_at_percent_rank_2,
    first_event_event_at,
    event_ticket_id,
    event_at,
    COALESCE(event_event_at_tercile, 0) AS event_event_at_tercile_filled
FROM with_details_event
ORDER BY event_at DESC, id
