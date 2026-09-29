WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 2 HOURS AS event_at_plus_2h
    FROM ticket_events
    WHERE actor = 'customer'
      AND event_type <> 'created'
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        DATEDIFF(DATE '2026-01-01', event_at_plus_2h) AS days_since_event_at_plus_2h
    FROM recent
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
    FROM scored s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT COUNT(CASE WHEN t.actor IS NOT NULL THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_actor_count,
        (SELECT COUNT(CASE WHEN t.event_type IN ('resolved', 'created') THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_event_type_count
    FROM profiled s
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM summarized_rows
    WHERE actor_len > (SELECT MIN(actor_len) FROM summarized_rows)
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.actor_len,
        s.days_since_event_at_plus_2h,
        s.event_count,
        s.event_actor_count,
        s.event_event_type_count,
        t.agent_id AS ticket_agent_id,
        t.opened_at AS ticket_opened_at
    FROM without_matches s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.opened_at < TIMESTAMP '2025-02-24 15:00:00'
),
latest AS (
    -- Keep the earliest row per actor
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        actor_len,
        days_since_event_at_plus_2h,
        event_count,
        event_actor_count,
        event_event_type_count,
        ticket_agent_id,
        ticket_opened_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor ORDER BY ticket_opened_at, id) AS recency_rank
        FROM with_details
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    event_count,
    event_actor_count,
    event_event_type_count,
    ticket_agent_id,
    ticket_opened_at
FROM latest
ORDER BY event_type, id
LIMIT 20
