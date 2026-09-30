WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        page,
        event_time,
        CEIL(duration_sec) duration_sec_whole,
        TO_DATE(event_time) event_time_date,
        COALESCE(page, 'OTHER') page_clean
    FROM web_events
    WHERE event_time >= DATE '2023-10-01'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(duration_sec_whole) OVER (PARTITION BY page ORDER BY duration_sec_whole, id) prev_duration_sec_whole,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec_whole, id) row_seq,
        AVG(duration_sec_whole) OVER (PARTITION BY DATE_TRUNC('YEAR', event_time), page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) duration_sec_whole_avg_7d
    FROM valid
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        page,
        event_time,
        event_time_date,
        page_clean,
        prev_duration_sec_whole,
        row_seq,
        duration_sec_whole_avg_7d,
        LEAD(prev_duration_sec_whole, 1) OVER w_recent next_prev_duration_sec_whole,
        SUM(duration_sec_whole_avg_7d) OVER (PARTITION BY page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) duration_sec_whole_avg_7d_sum_7d,
        ROW_NUMBER() OVER w_part event_seq,
        LEAD(duration_sec_whole, 2) OVER (PARTITION BY page ORDER BY duration_sec_whole_avg_7d, id) next_duration_sec_whole,
        COUNT(duration_sec_whole) OVER (PARTITION BY page) page_count_duration_sec_whole
    FROM metrics
    WINDOW w_recent AS (PARTITION BY page ORDER BY event_time_date, id), w_part AS (PARTITION BY page ORDER BY event_time_date DESC, id)
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM scored
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        page,
        event_time,
        event_time_date,
        page_clean,
        row_seq,
        page_count_duration_sec_whole,
        SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM sessionized
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM sessions_web_events
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessionized_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM gaps
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(row_seq, TRUE) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_row_seq,
        AVG(island_id) OVER (PARTITION BY DATE_TRUNC('YEAR', event_time), page ORDER BY session_start_flag, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) island_id_avg_2,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time_date DESC, id) event_seq,
        DENSE_RANK() OVER (PARTITION BY page ORDER BY is_new_burst NULLS LAST) is_new_burst_dense_rank
    FROM sessionized_web_events
    WHERE event_time_date <= '2023-04-10'
)
SELECT
    id,
    page,
    event_time_date,
    page_clean,
    row_seq,
    page_count_duration_sec_whole,
    is_new_burst,
    island_id,
    prev_event_time
FROM with_windows;
