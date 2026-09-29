WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        LOWER(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE MONTH(event_at) IN (2, 3)
      AND ticket_id >= 50
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        COUNT(CASE WHEN event_type IS NOT NULL THEN 1 END) AS event_event_type_count,
        MAX(event_at) AS last_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        COALESCE(t.event_event_type_count, 0) AS event_event_type_count,
        t.last_event_at_2
    FROM scoped_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM profiled s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
      AND s.last_event_at < TIMESTAMP '2025-02-13 09:00:00'
),
tagged AS (
    -- Derive helper columns
    SELECT *, DAYOFWEEK(last_event_at_2) AS last_event_at_2_dayofweek
    FROM with_activity
    WHERE MONTH(last_event_at) IN (2, 3)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, amount
FROM (
    SELECT id, last_event_at_2_dayofweek, event_event_type_count
    FROM tagged
) src
UNPIVOT (
    amount FOR metric IN (last_event_at_2_dayofweek AS m_last_event_at_2_dayofweek, event_event_type_count AS m_event_event_type_count)
) unp
ORDER BY id, metric
