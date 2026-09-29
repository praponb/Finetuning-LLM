WITH recent_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix,
        DATE_TRUNC('QUARTER', event_at) AS event_at_quarter,
        MONTH(event_at) AS event_at_month
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-03-08 09:00:00'
      AND event_type NOT IN ('replied', 'resolved')
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        MAX(event_at) AS last_event_at_2,
        COUNT(CASE WHEN actor IN ('system', 'agent') THEN 1 END) AS event_actor_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        t.last_event_at_2,
        COALESCE(t.event_actor_count, 0) AS event_actor_count
    FROM recent_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY event_actor_count, ticket_id ORDER BY event_actor_count DESC) AS event_actor_count_rank,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY DATE_TRUNC('MONTH', last_event_at_2), event_type)) AS distinct_ticket_id_in_last_event_at_2_month_even
    FROM with_stats
)
SELECT
    COALESCE(actor, 'ALL') AS actor,
    ticket_id,
    event_at_month,
    SUM(event_actor_count) AS total_event_actor_count,
    MAX(event_at_quarter) AS last_event_at_quarter
FROM sequenced
GROUP BY CUBE(actor, ticket_id, event_at_month)
ORDER BY actor NULLS LAST, ticket_id NULLS LAST, event_at_month NULLS LAST
