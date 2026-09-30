WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(event_type)) AS event_type_clean,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
    WHERE event_type <> 'resolved'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_clean,
        event_type_or_unknown,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        NTILE(5) OVER w_event_type_or_unknown_by_eve AS event_at_quintile,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_all_by_event_at)) / 60, 1) AS minutes_since_prev_event_at
    FROM valid
    WINDOW w_event_type_or_unknown_by_eve AS (PARTITION BY event_type_or_unknown ORDER BY event_at DESC, id), w_all_by_event_at AS (ORDER BY event_at DESC, id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        event_at_quintile,
        SUM(event_at_quintile) AS total_event_at_quintile,
        ROUND(SUM(event_at_quintile) / NULLIF(SUM(minutes_since_prev_event_at), 0), 3) AS event_at_quintile_to_minutes_since_prev_event_at,
        SUM(minutes_since_prev_event_at) AS total_minutes_since_prev_event_at,
        ROUND(STDDEV(minutes_since_prev_event_at), 2) AS stddev_minutes_since_prev_event_at
    FROM with_trends
    WHERE LENGTH(TRIM(event_type_clean)) >= 4
    GROUP BY 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT event_at_quintile, field, metric_value
FROM (
    SELECT event_at_quintile, stddev_minutes_since_prev_event_at, total_minutes_since_prev_event_at, event_at_quintile_to_minutes_since_prev_event_at
    FROM stats
) src
UNPIVOT (
    metric_value FOR field IN (stddev_minutes_since_prev_event_at AS stddev_minutes_since_prev_event_at_v, total_minutes_since_prev_event_at AS total_minutes_since_prev_event_at_v, event_at_quintile_to_minutes_since_prev_event_at AS m_event_at_quintile_to_minutes_since_prev_event_)
) unp
