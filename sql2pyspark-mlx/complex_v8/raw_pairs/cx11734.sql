WITH valid_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
        CONCAT_WS('|', event_type, actor) AS event_type_actor_label,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix
    FROM ticket_events
    WHERE event_type IS NOT NULL
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        event_at,
        event_type,
        actor,
        actor_prefix,
        LENGTH(TRIM(event_type_actor_label)) AS event_type_actor_label_len,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        QUARTER(event_at) AS event_at_quarter
    FROM valid_events
    WHERE actor = 'agent'
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM tagged
    WHERE event_type_actor_label_len > (SELECT AVG(event_type_actor_label_len) FROM tagged)
),
annotated AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(actor)) AS actor_len
    FROM with_activity
),
event_type_stats AS (
    -- Avg event_at_quarter per event_type
    SELECT event_type, AVG(event_at_quarter) AS avg_event_at_quarter
    FROM annotated
    GROUP BY event_type
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_event_at_quarter,
        ROUND(s.event_at_quarter - g.avg_event_at_quarter, 2) AS event_at_quarter_minus_avg
    FROM annotated s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.event_at_quarter > g.avg_event_at_quarter
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        COUNT(DISTINCT event_type_or_unknown) AS distinct_event_type_or_unknowns,
        MIN(avg_event_at_quarter) AS min_avg_event_at_quarter,
        ROUND(STDDEV(event_type_actor_label_len), 2) AS stddev_event_type_actor_label_len,
        ROUND(SUM(avg_event_at_quarter) / NULLIF(SUM(event_at_quarter_minus_avg), 0), 3) AS avg_event_at_quarter_to_event_at_quarter_minus_a,
        COUNT(DISTINCT actor) AS distinct_actors,
        SUM(event_type_actor_label_len) AS total_event_type_actor_label_len
    FROM compared
    WHERE TO_DATE(event_at) < DATE '2025-03-08'
    GROUP BY DATE_TRUNC('MONTH', event_at)
    HAVING AVG(event_at_quarter_minus_avg) > -0.8
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM totals
    WHERE total_event_type_actor_label_len < (SELECT MAX(total_event_type_actor_label_len) FROM totals)
)
-- Final projection
SELECT
    event_at_month,
    distinct_event_type_or_unknowns,
    avg_event_at_quarter_to_event_at_quarter_minus_a,
    distinct_actors,
    total_event_type_actor_label_len,
    COALESCE(distinct_actors, 0) AS distinct_actors_filled,
    event_at_month + INTERVAL 7 DAYS AS event_at_month_plus_7d
FROM filtered
WHERE distinct_actors <= 9
ORDER BY distinct_actors_filled DESC NULLS FIRST
