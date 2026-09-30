WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        DATE_TRUNC('QUARTER', opened_at) AS opened_at_quarter,
        LENGTH(TRIM(priority)) AS priority_len
    FROM tickets
    WHERE NOT ARRAY_CONTAINS(tags, 'login')
      AND channel IN ('phone', 'chat')
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.priority_len,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type
    FROM scoped s
    JOIN ticket_events t ON t.ticket_id = s.id
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM with_children s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.actor IS NOT NULL
              AND TO_DATE(t.event_at) < DATE '2025-02-06'
        )
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(priority_len - AVG(priority_len) OVER (PARTITION BY event_id), 2) AS priority_len_vs_event_id_avg,
        DENSE_RANK() OVER (PARTITION BY agent_id, channel ORDER BY priority_len ASC) AS priority_len_dense_rank,
        COUNT(*) OVER (PARTITION BY event_id) AS rows_in_event_id,
        ROUND(priority_len / NULLIF(SUM(priority_len) OVER (PARTITION BY event_id), 0), 4) AS priority_len_share_of_event_id
    FROM screened
)
SELECT
    id,
    agent_id,
    channel,
    priority_len,
    event_id,
    event_actor,
    event_type,
    priority_len_dense_rank,
    priority_len_share_of_event_id,
    COALESCE(rows_in_event_id, 0) AS rows_in_event_id_filled,
    LENGTH(TRIM(event_type)) AS event_type_len
FROM sequenced
ORDER BY priority_len_share_of_event_id NULLS LAST, priority_len
