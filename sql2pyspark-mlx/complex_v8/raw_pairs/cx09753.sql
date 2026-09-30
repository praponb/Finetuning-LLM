WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date,
        QUARTER(event_at) AS event_at_quarter,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label
    FROM ticket_events
    WHERE actor <> 'system'
      AND event_type = 'assigned'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        t.last_event_at
    FROM prepared_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at_2,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at_3
    FROM with_counts s
),
ticket_id_stats AS (
    -- Avg event_at_quarter per ticket_id
    SELECT ticket_id, AVG(event_at_quarter) AS avg_event_at_quarter
    FROM profiled
    GROUP BY ticket_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_event_at_quarter,
        ROUND(s.event_at_quarter - g.avg_event_at_quarter, 2) AS event_at_quarter_minus_avg
    FROM profiled s
    JOIN ticket_id_stats g ON g.ticket_id = s.ticket_id
    WHERE s.event_at_quarter > g.avg_event_at_quarter
),
per_group AS (
    -- Group-level totals
    SELECT
        event_type,
        event_at_quarter,
        actor,
        SUM(CASE WHEN event_type IN ('assigned', 'resolved') THEN 1 ELSE 0 END) AS n_event_type_in_assigned_resol,
        COUNT(*) AS cnt,
        SUM(event_at_quarter) AS total_event_at_quarter,
        SUM(event_at_quarter_minus_avg) FILTER (WHERE event_type IN ('replied', 'escalated', 'reopened')) AS event_at_quarter_minus_avg_event_type_in_replied,
        ROUND(SUM(event_count) / NULLIF(SUM(avg_event_at_quarter), 0), 3) AS event_count_to_avg_event_at_quarter_ratio
    FROM compared
    GROUP BY 1, 2, 3
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        event_type,
        event_at_quarter,
        actor,
        cnt,
        total_event_at_quarter,
        event_at_quarter_minus_avg_event_type_in_replied,
        MAX(event_at_quarter) OVER (ORDER BY total_event_at_quarter, event_type, event_at_quarter, actor ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS event_at_quarter_max_3_prior,
        SIZE(COLLECT_SET(event_type) OVER w_cnt) AS distinct_event_type_in_cnt,
        ROUND(CUME_DIST() OVER w_all_by_total_event_at_quarte_2, 3) AS total_event_at_quarter_cume_dist
    FROM per_group
    WINDOW w_cnt AS (PARTITION BY cnt), w_all_by_total_event_at_quarte_2 AS (ORDER BY total_event_at_quarter, event_type, event_at_quarter, actor)
)
-- Final projection
SELECT
    event_type,
    event_at_quarter,
    actor,
    cnt,
    total_event_at_quarter,
    event_at_quarter_minus_avg_event_type_in_replied,
    event_at_quarter_max_3_prior,
    distinct_event_type_in_cnt,
    total_event_at_quarter_cume_dist
FROM ranked
ORDER BY distinct_event_type_in_cnt
