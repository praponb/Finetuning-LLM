WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        event_at + INTERVAL 1 DAYS AS event_at_plus_1d
    FROM ticket_events
    WHERE event_type IN ('resolved', 'replied', 'reopened')
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM valid s
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        actor_or_unknown,
        last_event_at,
        event_count,
        NTH_VALUE(event_at_plus_1d, 2) OVER (ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_plus_1d,
        NTILE(4) OVER w_all_by_event_at AS event_at_quartile
    FROM with_counts
    WINDOW w_all_by_event_at AS (ORDER BY event_at DESC NULLS FIRST, id)
)
SELECT
    id,
    actor_or_unknown,
    last_event_at,
    second_event_at_plus_1d
FROM sequenced
ORDER BY id DESC
