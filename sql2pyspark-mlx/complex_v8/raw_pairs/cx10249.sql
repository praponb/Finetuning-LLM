WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE requester_id BETWEEN 7 AND 25
      AND HOUR(closed_at) BETWEEN 10 AND 15
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor
    FROM valid s
    JOIN ticket_events t ON t.ticket_id = s.id
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd') AS closed_at_label,
        DATE_TRUNC('HOUR', closed_at) AS closed_at_hour
    FROM line_level
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        event_id,
        event_actor,
        closed_at_label,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(closed_at_hour)) / 60, 1) AS minutes_closed_at_hour_to_event_at,
        DATE_FORMAT(closed_at, 'MMM yyyy') AS closed_at_label_2
    FROM scored
),
filtered AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
        )
)
SELECT
    id,
    event_id,
    event_actor,
    closed_at_label,
    closed_at_label_2,
    LOWER(TRIM(closed_at_label_2)) AS closed_at_label_2_clean,
    DATE_TRUNC('WEEK', closed_at) AS closed_at_week
FROM filtered
ORDER BY event_actor DESC NULLS FIRST, closed_at_week NULLS LAST, id, event_id
LIMIT 20
