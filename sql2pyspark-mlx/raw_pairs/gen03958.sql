WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), event_time) / 30 months_since_event_time,
        INITCAP(TRIM(event_type)) event_type_title,
        COALESCE(page, 'OTHER') page_clean
    FROM web_events
    WHERE event_time >= TO_DATE('2023-06-11')
      AND page IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(months_since_event_time, 1, 0) OVER (PARTITION BY session_id, event_type ORDER BY months_since_event_time, id) prev_months_since_event_time,
        LAST_VALUE(duration_sec, TRUE) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_duration_sec,
        COUNT(duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', event_time), event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) duration_sec_count_7d
    FROM valid
    WHERE months_since_event_time <= 51
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY months_since_event_time DESC NULLS FIRST, id) AS row_num
    FROM with_windows
    QUALIFY row_num = 1
),
with_windows_web_events AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        event_type_title,
        page_clean,
        last_known_duration_sec,
        row_num,
        FIRST_VALUE(months_since_event_time) IGNORE NULLS OVER (ORDER BY prev_months_since_event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_months_since_event_time,
        COUNT(last_known_duration_sec) OVER (PARTITION BY event_type) event_type_count_last_known_duration_sec,
        LAG(prev_months_since_event_time, 2, 0) OVER (PARTITION BY DATE_TRUNC('YEAR', event_time), event_type ORDER BY months_since_event_time, id) prev_prev_months_since_event_time,
        LAG(months_since_event_time, 1, 0) OVER (PARTITION BY session_id ORDER BY months_since_event_time, id) prev_months_since_event_time_2,
        LEAD(months_since_event_time, 1) OVER (PARTITION BY page ORDER BY duration_sec_count_7d, id) next_months_since_event_time
    FROM deduped
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN first_known_months_since_event_time >= 62 THEN 'High' WHEN first_known_months_since_event_time >= 10 THEN 'Medium' ELSE 'Low' END first_known_months_since_event_time_level,
        COALESCE(page, 'UNKNOWN') page_clean_2
    FROM with_windows_web_events
    WHERE first_known_months_since_event_time BETWEEN 43 AND 79
)
-- Join a dimension and aggregate
SELECT
    dim.device,
    dim.country,
    src.event_type,
    COUNT(*) AS rows_total,
    AVG(src.event_type_count_last_known_duration_sec) AS avg_event_type_count_last_known_duration_sec
FROM cleaned src
JOIN sessions dim ON src.session_id = dim.id
GROUP BY ROLLUP (dim.device, dim.country, src.event_type);
