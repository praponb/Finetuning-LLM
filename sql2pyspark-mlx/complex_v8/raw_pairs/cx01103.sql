WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('resolved', 'replied') THEN 'group_a' WHEN event_type = 'escalated' THEN 'group_b' ELSE 'other' END AS event_type_group,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix,
        CASE WHEN event_type IN ('replied', 'resolved') THEN 'group_a' WHEN event_type = 'created' THEN 'group_b' ELSE 'other' END AS event_type_group_2
    FROM ticket_events
    WHERE event_type = 'created'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(event_at) OVER (PARTITION BY event_type_group, ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type_group, ticket_id ORDER BY event_at), 3) AS event_at_cume_dist,
        NTILE(3) OVER w_event_type_group_ticket_id_b_3 AS event_at_tercile,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor, event_type ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at
    FROM valid
    WINDOW w_event_type_group_ticket_id_b_3 AS (PARTITION BY event_type_group, ticket_id ORDER BY event_at, id)
),
without_matches AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.priority NOT IN ('P3', 'P4')
        )
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_group,
        s.actor_prefix,
        s.event_at_cume_dist,
        s.event_at_tercile,
        s.minutes_since_prev_event_at,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor
    FROM without_matches s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type = 'assigned'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count
    FROM exploded_detail s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    event_type_group,
    actor_prefix,
    event_id,
    event_event_type,
    event_actor,
    event_count
FROM summarized_rows
ORDER BY event_type
