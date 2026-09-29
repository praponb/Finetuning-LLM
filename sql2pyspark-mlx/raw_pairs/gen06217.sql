WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_type,
        SUM(duration_sec) OVER (PARTITION BY TO_DATE(event_time), session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY DATE_TRUNC('YEAR', event_time), page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_2
    FROM web_events
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CAST(cumulative_duration_sec_2 AS DOUBLE) AS cumulative_duration_sec_2_num
    FROM with_windows
    WHERE event_type IN ('page_view', 'click')
),
base_web_events AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        event_type,
        cumulative_duration_sec_2_num,
        FLOOR(cumulative_duration_sec_2_num) AS cumulative_duration_sec_2_num_whole
    FROM base
    WHERE cumulative_duration_sec_2_num <= 67
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY cumulative_duration_sec_2_num_whole, id) AS row_num
    FROM base_web_events
    QUALIFY row_num = 1
)
SELECT
    id,
    event_type,
    cumulative_duration_sec_2_num_whole,
    row_num,
    CASE WHEN event_type RLIKE '^(click|page_view)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS event_type_zone
FROM deduped;
