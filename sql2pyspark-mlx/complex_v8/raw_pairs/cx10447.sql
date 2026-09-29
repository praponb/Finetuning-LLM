WITH valid_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor IN ('system', 'customer')
),
summary AS (
    -- Summarise per group
    SELECT
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_ym,
        ticket_id,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        ROUND(100 * COUNT(CASE WHEN actor = 'agent' THEN 1 END) / COUNT(*), 1) AS pct_actor_agent,
        COUNT(*) AS cnt,
        COUNT(DISTINCT actor) AS distinct_actors,
        MIN(event_at) AS first_event_at
    FROM valid_events
    GROUP BY actor, DATE_FORMAT(event_at, 'yyyy-MM'), ticket_id
    HAVING COUNT(*) >= 2
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('MONTH', first_event_at) AS first_event_at_month,
        distinct_actors - distinct_event_types AS distinct_actors_minus_distinct_event_types
    FROM summary
    WHERE pct_actor_agent <= 60.0
),
combined_details_ticket AS (
    -- Join tickets
    SELECT
        s.actor,
        s.event_at_ym,
        s.ticket_id,
        s.distinct_event_types,
        s.pct_actor_agent,
        s.cnt,
        s.distinct_actors,
        s.first_event_at,
        s.distinct_actors_minus_distinct_event_types,
        t.opened_at AS ticket_opened_at,
        t.tags AS ticket_tags
    FROM scored s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
joined_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.actor,
        s.event_at_ym,
        s.ticket_id,
        s.distinct_event_types,
        s.cnt,
        s.distinct_actors,
        s.first_event_at,
        s.distinct_actors_minus_distinct_event_types,
        s.ticket_opened_at,
        s.ticket_tags,
        t.closed_at AS ticket_closed_at
    FROM combined_details_ticket s
    JOIN tickets t ON t.id = s.ticket_id
),
line_level AS (
    -- One row per event
    SELECT
        s.actor,
        s.event_at_ym,
        s.ticket_id,
        s.distinct_event_types,
        s.distinct_actors_minus_distinct_event_types,
        s.ticket_closed_at,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor,
        t.event_at
    FROM joined_ticket s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
-- Final projection
SELECT
    actor,
    event_at_ym,
    distinct_event_types,
    distinct_actors_minus_distinct_event_types,
    ticket_closed_at,
    event_id,
    event_actor,
    SUBSTRING(TRIM(event_at_ym), 1, 2) AS event_at_ym_prefix,
    SUBSTRING(TRIM(event_actor), 1, 4) AS event_actor_prefix
FROM line_level
ORDER BY event_id DESC
