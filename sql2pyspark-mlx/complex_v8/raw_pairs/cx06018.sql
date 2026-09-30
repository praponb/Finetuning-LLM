WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-12'
),
without_matches AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM prepared_events s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.csat <= 5
              AND t.closed_at < TIMESTAMP '2025-02-21 07:00:00'
        )
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.days_since_event_at,
        s.event_type_or_unknown,
        s.event_at_label,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM without_matches s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
event_type_or_unknown_stats AS (
    -- Median days_since_event_at per event_type_or_unknown
    SELECT event_type_or_unknown, PERCENTILE_APPROX(days_since_event_at, 0.5) AS median_days_since_event_at
    FROM exploded_detail
    GROUP BY event_type_or_unknown
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_days_since_event_at,
        ROUND(s.days_since_event_at - g.median_days_since_event_at, 2) AS days_since_event_at_minus_median
    FROM exploded_detail s
    JOIN event_type_or_unknown_stats g ON g.event_type_or_unknown = s.event_type_or_unknown
    WHERE s.days_since_event_at >= g.median_days_since_event_at
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(median_days_since_event_at - AVG(median_days_since_event_at) OVER (PARTITION BY event_event_type), 2) AS median_days_since_event_at_vs_event_event_type_a,
        ROUND(AVG(days_since_event_at) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id, event_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS days_since_event_at_avg_7,
        ROW_NUMBER() OVER (PARTITION BY event_event_type, ticket_id ORDER BY days_since_event_at DESC, id, event_id) AS days_since_event_at_seq,
        SUM(median_days_since_event_at) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id, event_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS median_days_since_event_at_sum_3_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_event_type, ticket_id ORDER BY days_since_event_at DESC, id, event_id), 3) AS days_since_event_at_percent_rank,
        LAG(days_since_event_at, 1, 0) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id, event_id) AS prev_days_since_event_at
    FROM outliers
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.days_since_event_at,
        s.event_type_or_unknown,
        s.event_at_label,
        s.event_id,
        s.event_actor,
        s.event_event_type,
        s.median_days_since_event_at,
        s.days_since_event_at_seq,
        s.days_since_event_at_percent_rank,
        s.prev_days_since_event_at,
        t.event_at AS event_event_at_2,
        t.actor AS event_actor_2
    FROM metrics s
    LEFT JOIN ticket_events t ON t.id = s.event_id
)
SELECT *
FROM (
    SELECT event_actor, event_type_or_unknown, event_event_type, prev_days_since_event_at
    FROM enriched
) src
PIVOT (
    COUNT(prev_days_since_event_at)
    FOR event_event_type IN ('created', 'resolved', 'replied', 'reopened')
)
ORDER BY event_actor, event_type_or_unknown
