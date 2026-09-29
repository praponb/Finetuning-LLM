WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        SUBSTRING(TRIM(event_type), 1, 2) AS event_type_prefix,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
    FROM base_events
),
actor_stats AS (
    -- Median days_since_event_at per actor
    SELECT actor, PERCENTILE_APPROX(days_since_event_at, 0.5) AS median_days_since_event_at
    FROM annotated
    GROUP BY actor
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_days_since_event_at,
        ROUND(s.days_since_event_at - g.median_days_since_event_at, 2) AS days_since_event_at_minus_median
    FROM annotated s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.days_since_event_at >= g.median_days_since_event_at
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        event_type,
        actor,
        event_type_prefix,
        event_at_label,
        days_since_event_at_minus_median,
        ABS(days_since_event_at - 210) AS days_since_event_at_distance,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM vs_group
    WHERE days_since_event_at_minus_median > -560.0
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(days_since_event_at_distance - AVG(days_since_event_at_distance) OVER (PARTITION BY actor), 2) AS days_since_event_at_distance_vs_actor_avg,
        ROW_NUMBER() OVER (ORDER BY days_since_event_at_distance ASC, id) AS seq_no,
        MAX(days_since_event_at_distance) OVER (PARTITION BY event_type, actor_or_unknown ORDER BY days_since_event_at_distance DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS days_since_event_at_distance_max_4,
        SIZE(COLLECT_SET(actor_or_unknown) OVER w_actor) AS distinct_actor_or_unknown_in_actor,
        LEAD(days_since_event_at_distance, 1, 0) OVER (ORDER BY days_since_event_at_distance ASC, id) AS next_days_since_event_at_distance,
        ROUND(CUME_DIST() OVER w_event_type_by_days_since_eve, 3) AS days_since_event_at_minus_median_cume_dist
    FROM derived
    WINDOW w_actor AS (PARTITION BY actor), w_event_type_by_days_since_eve AS (PARTITION BY event_type ORDER BY days_since_event_at_minus_median DESC, id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT actor_or_unknown, event_type, distinct_actor_or_unknown_in_actor
    FROM windowed
) src
PIVOT (
    MAX(distinct_actor_or_unknown_in_actor) AS max_v, SUM(distinct_actor_or_unknown_in_actor) AS sum_v
    FOR event_type IN ('resolved', 'assigned', 'escalated')
)
ORDER BY actor_or_unknown
