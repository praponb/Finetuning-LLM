WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CASE WHEN requester_id >= 20 THEN 'A' WHEN requester_id >= 7 THEN 'B' ELSE 'C' END AS requester_id_tier,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 6 AND 10
      AND agent_id <= 4
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
    FROM cleaned_tickets s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
screened AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM summarized_rows s
    WHERE s.agent_id IN (
            SELECT t.agent_id
            FROM tickets t
            WHERE t.csat <= 3
              AND NOT ARRAY_CONTAINS(t.tags, 'outage')
        )
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        requester_id_tier,
        requester_id_bucket,
        NTILE(4) OVER w_agent_id_channel_by_requeste AS requester_id_quartile,
        requester_id - LAG(requester_id) OVER (PARTITION BY YEAR(closed_at), event_count ORDER BY requester_id, id) AS requester_id_change,
        LAST_VALUE(event_count) OVER (PARTITION BY agent_id, channel ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_count
    FROM screened
    WINDOW w_agent_id_channel_by_requeste AS (PARTITION BY agent_id, channel ORDER BY requester_id DESC, id)
)
-- Final projection
SELECT
    id,
    priority,
    requester_id_tier,
    requester_id_bucket,
    requester_id_quartile,
    requester_id_change,
    DATE_TRUNC('DAY', opened_at) AS opened_at_day,
    CONCAT(channel, '#', CAST(requester_id_quartile AS STRING)) AS channel_ref
FROM windowed
WHERE requester_id_tier = 'B'
ORDER BY requester_id_bucket NULLS LAST, id DESC
