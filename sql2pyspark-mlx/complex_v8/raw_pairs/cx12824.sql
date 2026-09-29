WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
        DATEDIFF(DATE '2025-12-31', closed_at) AS days_since_closed_at
    FROM tickets
    WHERE MONTH(closed_at) IN (2, 3)
      AND csat < 5
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.channel_slug,
        s.days_since_closed_at,
        t.id AS event_id,
        t.event_at,
        t.event_type,
        t.actor AS event_actor
    FROM cleaned s
    JOIN ticket_events t ON t.ticket_id = s.id
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM detail_rows_events s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
agent_id_stats AS (
    -- Avg requester_id per agent_id
    SELECT agent_id, AVG(requester_id) AS avg_requester_id
    FROM with_stats
    GROUP BY agent_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_requester_id,
        ROUND(s.requester_id - g.avg_requester_id, 2) AS requester_id_minus_avg
    FROM with_stats s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.requester_id > g.avg_requester_id
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_ym,
        event_id,
        ticket_count,
        ROUND(STDDEV(requester_id), 2) AS stddev_requester_id,
        ROUND(AVG(avg_requester_id), 2) AS avg_avg_requester_id,
        COUNT(*) AS row_count,
        SUM(requester_id_minus_avg) AS total_requester_id_minus_avg
    FROM compared
    GROUP BY DATE_FORMAT(event_at, 'yyyy-MM'), event_id, ticket_count
)
-- Final projection
SELECT
    event_at_ym,
    event_id,
    ticket_count,
    avg_avg_requester_id,
    row_count,
    total_requester_id_minus_avg,
    CASE WHEN avg_avg_requester_id >= 17.8 THEN 'large' WHEN avg_avg_requester_id >= 9.4 THEN 'medium' ELSE 'small' END AS avg_avg_requester_id_tier,
    LENGTH(TRIM(event_at_ym)) AS event_at_ym_len
FROM totals
WHERE total_requester_id_minus_avg <= 88.8
