WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        tags,
        UPPER(TRIM(priority)) AS priority_clean,
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'refund')
      AND priority <> 'P2'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at
    FROM cleaned s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
deduped AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        tags,
        priority_clean,
        tags_text,
        first_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY requester_id DESC, id DESC) AS rn
        FROM with_stats
    ) ranked
    WHERE rn = 1
)
SELECT
    COALESCE(channel, 'ALL') AS channel,
    COALESCE(priority, 'ALL') AS priority,
    GROUPING(channel) AS is_total_channel,
    MIN(closed_at) AS first_closed_at
FROM deduped
GROUP BY ROLLUP(channel, priority)
