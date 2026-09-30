WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-19 11:00:00'
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        event_at,
        event_type,
        LOWER(REGEXP_REPLACE(TRIM(event_type_or_unknown), ' ', '_')) AS event_type_or_unknown_slug,
        CASE WHEN actor IN ('system', 'customer') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM filtered_events
    WHERE actor = 'customer'
),
event_type_stats AS (
    -- Avg days_since_event_at per event_type
    SELECT event_type, AVG(days_since_event_at) AS avg_days_since_event_at
    FROM scored
    GROUP BY event_type
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_event_at,
        ROUND(s.days_since_event_at - g.avg_days_since_event_at, 2) AS days_since_event_at_minus_avg
    FROM scored s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.days_since_event_at > g.avg_days_since_event_at
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM vs_group
    WHERE days_since_event_at_minus_avg > (SELECT AVG(days_since_event_at_minus_avg) FROM vs_group)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        days_since_event_at - avg_days_since_event_at AS days_since_event_at_minus_avg_days_since_event_a
    FROM with_activity
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        event_at,
        event_type_or_unknown_slug,
        actor_group,
        days_since_event_at,
        avg_days_since_event_at,
        days_since_event_at_minus_avg,
        SUM(avg_days_since_event_at) OVER (PARTITION BY event_type ORDER BY days_since_event_at_minus_avg DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS avg_days_since_event_at_sum_1_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY days_since_event_at_minus_avg DESC, id), 3) AS days_since_event_at_minus_avg_cume_dist,
        FIRST_VALUE(days_since_event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY days_since_event_at_minus_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_event_at,
        COUNT(*) OVER w_event_at_year AS rows_in_event_at_year
    FROM derived
    WINDOW w_event_at_year AS (PARTITION BY YEAR(event_at))
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type_or_unknown_slug,
        actor_group,
        days_since_event_at,
        avg_days_since_event_at,
        days_since_event_at_minus_avg,
        days_since_event_at_minus_avg_cume_dist,
        first_known_days_since_event_at,
        rows_in_event_at_year,
        COUNT(*) OVER (PARTITION BY actor_group) AS rows_in_actor_group,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor_group ORDER BY days_since_event_at_minus_avg ASC), 3) AS days_since_event_at_minus_avg_percent_rank,
        ROUND(AVG(avg_days_since_event_at) OVER w_actor_group, 2) AS actor_group_avg_avg_days_since_event_at,
        NTH_VALUE(days_since_event_at, 2) OVER (ORDER BY avg_days_since_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_event_at
    FROM metrics
    WINDOW w_actor_group AS (PARTITION BY actor_group)
)
SELECT
    id,
    event_at,
    event_type_or_unknown_slug,
    actor_group,
    days_since_event_at,
    avg_days_since_event_at,
    days_since_event_at_minus_avg,
    days_since_event_at_minus_avg_cume_dist,
    first_known_days_since_event_at,
    rows_in_event_at_year,
    rows_in_actor_group,
    days_since_event_at_minus_avg_percent_rank,
    actor_group_avg_avg_days_since_event_at,
    second_days_since_event_at,
    DAYOFWEEK(event_at) AS event_at_dayofweek
FROM windowed
ORDER BY days_since_event_at_minus_avg DESC
