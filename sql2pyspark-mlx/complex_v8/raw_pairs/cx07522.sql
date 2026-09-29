WITH cleaned_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor <> 'customer'
      AND event_type <> 'escalated'
),
without_matches AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM cleaned_events s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.channel = 'chat'
              AND t.opened_at < TIMESTAMP '2025-02-24 15:00:00'
        )
),
derived AS (
    -- Add calculated columns
    SELECT *, DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM without_matches
    WHERE ticket_id IS NOT NULL
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) AS event_count,
        COUNT(*) FILTER (WHERE actor = 'agent') AS event_actor_count,
        MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_actor_count, 0) AS event_actor_count,
        t.last_event_at
    FROM derived s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(event_count / NULLIF(SUM(event_count) OVER w_event_at_month_event_type, 0), 4) AS event_count_share_of_event_at_month_event_type,
        MAX(days_since_event_at) OVER (PARTITION BY ticket_id, event_actor_count ORDER BY event_actor_count DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS days_since_event_at_max_2_prior
    FROM summarized_rows
    WINDOW w_event_at_month_event_type AS (PARTITION BY MONTH(event_at), event_type)
)
SELECT DISTINCT days_since_event_at_max_2_prior, ticket_id, event_at
FROM with_windows
ORDER BY ticket_id DESC NULLS FIRST
