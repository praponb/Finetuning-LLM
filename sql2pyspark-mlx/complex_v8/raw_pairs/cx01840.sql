WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
    FROM ticket_events
    WHERE event_type IN ('replied', 'reopened')
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM valid s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
),
tagged AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(event_at_label)) AS event_at_label_len
    FROM qualified
),
summary AS (
    -- Summarise per group
    SELECT
        ticket_id,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids,
        SUM(event_at_label_len) AS total_event_at_label_len
    FROM tagged
    GROUP BY ticket_id
    HAVING SUM(event_at_label_len) > 6
),
detail_rows AS (
    -- One row per event
    SELECT
        s.ticket_id,
        s.total_event_at_label_len,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor
    FROM summary s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
SELECT
    event_id,
    ticket_id,
    SUM(total_event_at_label_len) AS total_total_event_at_label_len,
    MAX(event_at) AS last_event_at
FROM detail_rows
GROUP BY CUBE(event_id, ticket_id)
