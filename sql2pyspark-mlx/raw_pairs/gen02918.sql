WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        event_type,
        page,
        duration_sec,
        LENGTH(event_type) AS event_type_length,
        COALESCE(page, 'N/A') AS page_filled
    FROM web_events
    WHERE page IS DISTINCT FROM '/checkout'
      AND event_time >= '2025-09-14'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(duration_sec) OVER (PARTITION BY event_type ORDER BY event_type_length, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_duration_sec,
        AVG(duration_sec) OVER (PARTITION BY event_type ORDER BY event_type_length, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_duration_sec,
        MAX(duration_sec) OVER (PARTITION BY event_type ORDER BY event_type_length, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY event_type_length, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_duration_sec
    FROM valid
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        event_type,
        page,
        duration_sec,
        event_type_length,
        page_filled,
        last7_min_duration_sec,
        last7_max_duration_sec,
        last7_sum_duration_sec,
        last7_min_duration_sec / NULLIF(last7_max_duration_sec, 0) AS last7_min_duration_sec_per_last7_max_duration_sec
    FROM with_windows
    WHERE page IS DISTINCT FROM '/checkout'
      AND last7_sum_duration_sec <= 1500
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY page, event_type ORDER BY last7_min_duration_sec DESC NULLS FIRST) AS row_num
    FROM base
    QUALIFY row_num = 1
)
SELECT
    id,
    page,
    event_type_length,
    page_filled,
    last7_min_duration_sec,
    last7_max_duration_sec,
    last7_sum_duration_sec,
    last7_min_duration_sec_per_last7_max_duration_sec,
    row_num
FROM latest
ORDER BY event_type_length
