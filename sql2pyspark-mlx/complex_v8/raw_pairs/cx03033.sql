WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        YEAR(event_at) AS event_at_year,
        QUARTER(event_at) AS event_at_quarter
    FROM ticket_events
    WHERE event_type = 'reopened'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY event_at_quarter) AS rows_in_event_at_quarter,
        SIZE(COLLECT_SET(event_type) OVER w_event_at_quarter) AS distinct_event_type_in_event_at_quarter,
        ROUND(AVG(event_at_quarter) OVER w_event_at_quarter, 2) AS event_at_quarter_avg_event_at_quarter
    FROM prepared_events
    WINDOW w_event_at_quarter AS (PARTITION BY event_at_quarter)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        days_since_event_at,
        event_at_year,
        event_at_quarter_avg_event_at_quarter,
        MAX(days_since_event_at) OVER (PARTITION BY distinct_event_type_in_event_at_quarter ORDER BY event_at_quarter_avg_event_at_quarter DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS days_since_event_at_max_1_prior,
        LEAD(event_at_quarter) OVER w_event_at_quarter_by_rows_in AS next_event_at_quarter,
        ROUND(rows_in_event_at_quarter / NULLIF(SUM(rows_in_event_at_quarter) OVER (PARTITION BY event_at_quarter), 0), 4) AS rows_in_event_at_quarter_share_of_event_at_quart
    FROM with_trends
    WINDOW w_event_at_quarter_by_rows_in AS (PARTITION BY event_at_quarter ORDER BY rows_in_event_at_quarter, id)
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(days_since_event_at / NULLIF(SUM(days_since_event_at) OVER w_event_at_year_event_type, 0), 4) AS days_since_event_at_share_of_event_at_year_event,
        FIRST_VALUE(event_at) OVER (ORDER BY days_since_event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        FIRST_VALUE(days_since_event_at) OVER (PARTITION BY ticket_id, event_at_year ORDER BY event_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_days_since_event_at
    FROM sequenced
    WINDOW w_event_at_year_event_type AS (PARTITION BY event_at_year, event_type)
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count_2
    FROM sequenced_2 s
),
actor_stats AS (
    -- Avg event_count_2 per actor
    SELECT actor, AVG(event_count_2) AS avg_event_count_2
    FROM profiled
    GROUP BY actor
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_event_count_2,
        ROUND(s.event_count_2 - g.avg_event_count_2, 2) AS event_count_2_minus_avg
    FROM profiled s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.event_count_2 > g.avg_event_count_2
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count_3,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count_4
    FROM vs_group s
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    event_at_year,
    days_since_event_at_share_of_event_at_year_event,
    first_days_since_event_at,
    event_count_2,
    avg_event_count_2,
    event_count_3,
    event_count_4,
    ABS(event_count_2_minus_avg - 1.6) AS event_count_2_minus_avg_distance,
    COALESCE(days_since_event_at, 0) AS days_since_event_at_filled
FROM with_counts
WHERE event_count < 7
ORDER BY event_at NULLS LAST
