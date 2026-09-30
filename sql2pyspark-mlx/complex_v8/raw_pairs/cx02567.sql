WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_type = 'created'
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM scoped_events s
),
without_matches AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM summarized_rows s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.priority <> 'P4'
              AND t.channel = 'web'
        )
),
enriched AS (
    -- Add calculated columns
    SELECT *, LOWER(TRIM(actor)) AS actor_clean
    FROM without_matches
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        event_at,
        event_type,
        actor,
        first_event_at,
        event_count,
        actor_clean,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(first_event_at)) / 60, 1) AS minutes_first_event_at_to_event_at,
        DATE_FORMAT(first_event_at, 'yyyy-MM-dd') AS first_event_at_label,
        INITCAP(TRIM(actor)) AS actor_clean_2
    FROM enriched
    WHERE event_at < TIMESTAMP '2025-03-02 09:00:00'
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    first_event_at,
    event_count,
    actor_clean,
    minutes_first_event_at_to_event_at,
    first_event_at_label,
    actor_clean_2,
    CASE WHEN event_type IN ('created', 'escalated') THEN 'group_a' WHEN event_type = 'reopened' THEN 'group_b' ELSE 'other' END AS event_type_group,
    COALESCE(event_count, 0) AS event_count_filled
FROM labeled
ORDER BY id NULLS LAST
