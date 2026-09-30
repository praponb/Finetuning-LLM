WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-24'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY event_at), 3) AS event_at_percent_rank,
        LAG(event_at) OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id) AS prev_event_at,
        NTILE(3) OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id) AS event_at_tercile,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_event_type_actor_by_event_at)) / 60, 1) AS minutes_since_prev_event_at
    FROM valid
    WINDOW w_event_type_actor_by_event_at AS (PARTITION BY event_type, actor ORDER BY event_at DESC, id)
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.event_type_or_unknown,
        s.event_at_label,
        s.prev_event_at,
        s.last_known_event_at,
        s.minutes_since_prev_event_at,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM sequenced s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        event_type_or_unknown,
        DATE_TRUNC('MONTH', event_event_at) AS event_event_at_month,
        SUM(minutes_since_prev_event_at) AS total_minutes_since_prev_event_at,
        SUM(CASE WHEN actor IS NOT NULL THEN minutes_since_prev_event_at ELSE 0 END) AS minutes_since_prev_event_at_actor_is_not_null,
        COUNT(*) FILTER (WHERE event_event_type NOT IN ('created', 'assigned')) AS n_event_event_type_not_in_crea,
        PERCENTILE_APPROX(minutes_since_prev_event_at, 0.5) AS median_minutes_since_prev_event_at,
        COUNT(*) AS cnt
    FROM exploded_detail
    WHERE event_event_at >= TIMESTAMP '2025-01-25 13:00:00'
    GROUP BY event_type_or_unknown, DATE_TRUNC('MONTH', event_event_at)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM totals
    WHERE total_minutes_since_prev_event_at > (SELECT AVG(total_minutes_since_prev_event_at) FROM totals)
)
SELECT DISTINCT minutes_since_prev_event_at_actor_is_not_null, cnt
FROM with_activity
WHERE n_event_event_type_not_in_crea <= 6
