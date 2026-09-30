WITH joined_ranges AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        CASE WHEN hours_since_open >= 39.6 THEN 'large' WHEN hours_since_open >= 18.0 THEN 'medium' ELSE 'small' END AS hours_since_open_tier
    FROM joined_ranges
),
priority_stats AS (
    -- Avg days_since_event_at per priority
    SELECT priority, AVG(days_since_event_at) AS avg_days_since_event_at
    FROM scored
    GROUP BY priority
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_event_at,
        ROUND(s.days_since_event_at - g.avg_days_since_event_at, 2) AS days_since_event_at_minus_avg
    FROM scored s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.days_since_event_at > g.avg_days_since_event_at
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, avg_days_since_event_at, hours_since_open, days_since_event_at_minus_avg
    FROM vs_group
) src
UNPIVOT (
    reading FOR measure IN (avg_days_since_event_at AS m_avg_days_since_event_at, hours_since_open AS hours_since_open_v, days_since_event_at_minus_avg AS days_since_event_at_minus_avg_v)
) unp
ORDER BY id, measure
