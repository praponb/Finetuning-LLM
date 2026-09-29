WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        DATEDIFF(DATE '2025-12-31', closed_at) AS days_since_closed_at,
        GREATEST(requester_id, csat) AS greatest_requester_id_csat,
        MONTH(opened_at) AS opened_at_month
    FROM tickets
    WHERE agent_id >= 3
),
enriched AS (
    -- Attach agent attributes
    SELECT
        s.*,
        su.team AS agent_team,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM prepared s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
priority_stats AS (
    -- Avg greatest_requester_id_csat per priority
    SELECT priority, AVG(greatest_requester_id_csat) AS avg_greatest_requester_id_csat
    FROM enriched
    GROUP BY priority
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_greatest_requester_id_csat,
        ROUND(s.greatest_requester_id_csat - g.avg_greatest_requester_id_csat, 2) AS greatest_requester_id_csat_minus_avg
    FROM enriched s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.greatest_requester_id_csat > g.avg_greatest_requester_id_csat
),
per_label AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.csat,
        s.days_since_closed_at,
        s.greatest_requester_id_csat,
        s.opened_at_month,
        s.agent_team,
        s.agent_name,
        s.agent_hired_on,
        s.avg_greatest_requester_id_csat,
        s.greatest_requester_id_csat_minus_avg,
        t.tag_pos,
        t.tag
    FROM above_average s
    LATERAL VIEW OUTER POSEXPLODE(s.tags) t AS tag_pos, tag
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        t.last_event_at
    FROM per_label s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    priority,
    csat,
    days_since_closed_at,
    opened_at_month,
    agent_hired_on,
    tag_pos,
    tag,
    last_event_at,
    ROUND((UNIX_TIMESTAMP(last_event_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_last_event_at
FROM with_stats
ORDER BY csat DESC, id, tag_pos
LIMIT 5
