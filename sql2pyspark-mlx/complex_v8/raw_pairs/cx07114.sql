WITH scoped AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id <= 26
),
tagged AS (
    -- Label and score rows
    SELECT *, INITCAP(TRIM(actor)) AS actor_clean
    FROM scoped
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.actor_clean,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM tagged s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM detail_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_at >= TIMESTAMP '2025-01-25 08:00:00'
              AND t.actor <> 'agent'
        )
),
stats AS (
    -- Group-level totals
    SELECT
        ticket_id,
        DATE_FORMAT(event_event_at, 'yyyy-MM') AS event_event_at_ym,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(event_event_type)) AS event_event_type_list
    FROM without_matches
    GROUP BY ticket_id, DATE_FORMAT(event_event_at, 'yyyy-MM')
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM stats
    WHERE cnt > (SELECT MIN(cnt) FROM stats)
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.event_type NOT IN ('escalated', 'reopened')) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_event_type_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM screened s
)
SELECT
    ticket_id,
    event_event_at_ym,
    actor_list,
    cnt,
    event_event_type_list,
    event_event_type_count,
    event_count,
    SUBSTRING(TRIM(event_event_at_ym), 1, 3) AS event_event_at_ym_prefix
FROM with_stats
ORDER BY ticket_id, cnt DESC NULLS FIRST
