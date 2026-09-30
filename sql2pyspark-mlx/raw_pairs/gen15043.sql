WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        page,
        duration_sec,
        SUM(duration_sec) OVER (PARTITION BY YEAR(event_time), page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        CUME_DIST() OVER (PARTITION BY YEAR(event_time), session_id ORDER BY duration_sec NULLS FIRST) AS duration_sec_cume_dist
    FROM web_events
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(running_duration_sec) OVER (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec_cume_dist DESC, id) AS row_seq
    FROM metrics
    WHERE session_id IN (7, 16, 33, 35)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        page,
        running_duration_sec,
        duration_sec_cume_dist,
        cumulative_running_duration_sec,
        row_seq,
        FLOOR(cumulative_running_duration_sec) AS cumulative_running_duration_sec_whole,
        ROUND(row_seq, 1) AS row_seq_rounded,
        COALESCE(page, 'UNKNOWN') AS page_clean
    FROM scored
    WHERE page IS NOT NULL
      AND cumulative_running_duration_sec < 5044
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, page
    FROM cleaned
    EXCEPT
    SELECT id, page
    FROM web_events
    WHERE session_id IN (1, 14, 20)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.page = k.page
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        page,
        running_duration_sec,
        duration_sec_cume_dist,
        cumulative_running_duration_sec_whole,
        row_seq_rounded,
        page_clean,
        ROUND(duration_sec_cume_dist * 1.1, 2) AS duration_sec_cume_dist_scaled,
        cumulative_running_duration_sec + cumulative_running_duration_sec_whole AS cumulative_running_duration_sec_plus_cumulative_running_duration_sec_whole
    FROM verified
    WHERE row_seq_rounded <= 46
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        session_id,
        duration_sec_cume_dist_scaled,
        row_seq_rounded,
        cumulative_running_duration_sec_plus_cumulative_running_duration_sec_whole
    FROM filtered
) src
UNPIVOT (
    reading FOR measure IN (duration_sec_cume_dist_scaled, row_seq_rounded, cumulative_running_duration_sec_plus_cumulative_running_duration_sec_whole)
) u;
