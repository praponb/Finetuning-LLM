WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        tags,
        DATEDIFF(DATE '2026-01-01', closed_at) AS days_since_closed_at,
        DATE_TRUNC('QUARTER', closed_at) AS closed_at_quarter,
        CASE WHEN csat >= 4 THEN 'large' WHEN csat >= 2 THEN 'medium' ELSE 'small' END AS csat_tier
    FROM tickets
    WHERE priority <> 'P1'
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS('-', channel, priority) AS channel_priority_label,
        COALESCE(csat_tier, 'UNKNOWN') AS csat_tier_or_unknown
    FROM cleaned
    WHERE days_since_closed_at > 280
),
csat_tier_or_unknown_stats AS (
    -- Avg requester_id per csat_tier_or_unknown
    SELECT csat_tier_or_unknown, AVG(requester_id) AS avg_requester_id
    FROM tagged
    GROUP BY csat_tier_or_unknown
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_requester_id,
        ROUND(s.requester_id - g.avg_requester_id, 2) AS requester_id_minus_avg
    FROM tagged s
    JOIN csat_tier_or_unknown_stats g ON g.csat_tier_or_unknown = s.csat_tier_or_unknown
    WHERE s.requester_id > g.avg_requester_id
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.tags,
        s.csat_tier,
        s.channel_priority_label,
        s.csat_tier_or_unknown,
        s.requester_id_minus_avg,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type,
        t.event_at
    FROM vs_group s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM line_level_events s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_type = 'created'
              AND t.event_at >= TIMESTAMP '2025-02-06 12:00:00'
        )
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    csat_tier,
    channel_priority_label,
    csat_tier_or_unknown,
    requester_id_minus_avg,
    event_id,
    event_actor,
    event_type,
    ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(closed_at)) / 60, 1) AS minutes_closed_at_to_event_at,
    COALESCE(csat_tier, 'UNKNOWN') AS csat_tier_or_unknown_2
FROM filtered
WHERE requester_id_minus_avg BETWEEN -10.0 AND 12.5
ORDER BY csat_tier DESC, csat_tier_or_unknown_2 DESC
