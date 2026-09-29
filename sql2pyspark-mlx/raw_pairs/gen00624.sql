WITH cleaned AS (
    SELECT
        id,
        session_id,
        event_time,
        duration_sec,
        CASE WHEN duration_sec >= 511 THEN 'High' WHEN duration_sec >= 123 THEN 'Medium' ELSE 'Low' END AS duration_sec_tier,
        INITCAP(TRIM(event_type)) AS event_type_title
    FROM web_events
    WHERE page IS NOT NULL
),
base AS (
    SELECT
        id,
        session_id,
        duration_sec_tier,
        DATEDIFF(CURRENT_DATE(), event_time) / 30 AS months_since_event_time,
        DATE_TRUNC('YEAR', event_time) AS event_time_period
    FROM cleaned
    WHERE TO_DATE(event_time) < '2023-12-01'
),
scored AS (
    SELECT
        id,
        duration_sec_tier,
        event_time_period,
        FIRST_VALUE(months_since_event_time) IGNORE NULLS OVER (PARTITION BY session_id ORDER BY event_time_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_months_since_event_time,
        LAST_VALUE(months_since_event_time) IGNORE NULLS OVER (PARTITION BY session_id ORDER BY months_since_event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_months_since_event_time
    FROM base
),
scored_web_events AS (
    SELECT
        id,
        duration_sec_tier,
        event_time_period,
        last_known_months_since_event_time,
        MAX(last_known_months_since_event_time) OVER (ORDER BY first_known_months_since_event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_last_known_months_since_event_time,
        SUM(last_known_months_since_event_time) OVER (ORDER BY first_known_months_since_event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_last_known_months_since_event_time,
        AVG(last_known_months_since_event_time) OVER (ORDER BY first_known_months_since_event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_last_known_months_since_event_time,
        COUNT(last_known_months_since_event_time) OVER (ORDER BY first_known_months_since_event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_last_known_months_since_event_time
    FROM scored
)
SELECT
    id,
    event_time_period,
    last_known_months_since_event_time,
    running_max_last_known_months_since_event_time,
    running_sum_last_known_months_since_event_time,
    running_count_last_known_months_since_event_time
FROM scored_web_events
ORDER BY running_max_last_known_months_since_event_time;
