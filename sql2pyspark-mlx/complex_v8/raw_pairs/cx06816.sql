WITH recent AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        csat,
        CONCAT_WS('-', channel, priority) AS channel_priority_label,
        SIZE(tags) AS tags_count
    FROM tickets
    WHERE channel = 'web'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count
    FROM recent s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN requester_id >= 20 THEN 'A' WHEN requester_id >= 5 THEN 'B' ELSE 'C' END AS requester_id_tier,
        SUBSTRING(TRIM(priority), 1, 3) AS priority_prefix
    FROM with_counts
    WHERE event_count < 7
),
without_matches AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM enriched s
    WHERE s.agent_id IN (
            SELECT t.agent_id
            FROM tickets t
            WHERE TO_DATE(t.opened_at) < DATE '2025-02-12'
              AND t.requester_id >= 5
        )
)
-- Subtotals with CUBE
SELECT
    COALESCE(priority, 'ALL') AS priority,
    tags_count,
    COALESCE(requester_id_tier, 'ALL') AS requester_id_tier,
    GROUPING_ID() AS gid,
    SUM(tags_count) AS total_tags_count,
    SUM(CASE WHEN priority = 'P2' THEN requester_id ELSE 0 END) AS requester_id_priority_p2,
    ARRAY_SORT(COLLECT_SET(priority)) AS priority_list
FROM without_matches
GROUP BY CUBE(priority, tags_count, requester_id_tier)
