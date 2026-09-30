WITH valid_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('reopened', 'created') THEN 'group_a' WHEN event_type = 'assigned' THEN 'group_b' ELSE 'other' END AS event_type_group,
        LENGTH(TRIM(event_type)) AS event_type_len,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix
    FROM ticket_events
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) AS event_count,
        MIN(event_at) AS first_event_at,
        COUNT(*) AS event_count_2
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        t.first_event_at,
        COALESCE(t.event_count_2, 0) AS event_count_2
    FROM valid_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
with_counts_2 AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN t.event_type <> 'resolved' THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_event_type_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at_2
    FROM with_counts s
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_group,
        event_type_len,
        actor_prefix,
        first_event_at,
        event_count_2,
        event_event_type_count,
        last_event_at,
        first_event_at_2,
        COUNT(*) OVER w_event_type_group AS rows_in_event_type_group,
        ROW_NUMBER() OVER (ORDER BY first_event_at DESC, id) AS seq_no,
        MIN(event_count) OVER (PARTITION BY event_type_group) AS event_type_group_min_event_count
    FROM with_counts_2
    WINDOW w_event_type_group AS (PARTITION BY event_type_group)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    event_type_group,
    event_type_len,
    first_event_at,
    event_count_2,
    event_event_type_count,
    last_event_at,
    first_event_at_2,
    rows_in_event_type_group,
    seq_no,
    event_type_group_min_event_count
FROM metrics
ORDER BY event_count_2 DESC, event_type_group DESC NULLS FIRST
