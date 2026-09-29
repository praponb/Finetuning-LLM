WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM tickets
    WHERE (agent_id IS NOT NULL OR csat < 4)
),
filtered AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM filtered_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.actor <> 'system'
        )
      AND ARRAY_CONTAINS(s.tags, 'bug')
),
channel_stats AS (
    -- Max csat per channel
    SELECT channel, MAX(csat) AS max_csat
    FROM filtered
    GROUP BY channel
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_csat, ROUND(s.csat - g.max_csat, 2) AS csat_minus_max
    FROM filtered s
    JOIN channel_stats g ON g.channel = s.channel
    WHERE s.csat = g.max_csat
)
SELECT
    agent_id,
    COALESCE(channel, 'ALL') AS channel,
    COALESCE(priority, 'ALL') AS priority,
    GROUPING(agent_id) AS is_total_agent_id,
    ROUND(SUM(max_csat) / NULLIF(SUM(csat), 0), 3) AS max_csat_to_csat_ratio,
    COUNT(DISTINCT id) AS distinct_ids
FROM compared
GROUP BY CUBE(agent_id, channel, priority)
